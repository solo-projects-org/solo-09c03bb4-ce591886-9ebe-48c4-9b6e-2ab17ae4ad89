import csv
import io
import shutil
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    shutil.copy(Path("samples/inputs/lims.csv"), inputs / "lims.csv")
    shutil.copy(Path("samples/inputs/initial_consent.csv"), inputs / "initial_consent.csv")
    return tmp_path


def make_app(workdir: Path, row_delay: float = 0.0):
    settings = Settings(
        input_csv=workdir / "inputs/lims.csv",
        initial_consent_csv=workdir / "inputs/initial_consent.csv",
        data_dir=workdir / "data",
        row_delay_seconds=row_delay,
        check_interval_rows=1,
    )
    return create_app(settings), settings


@pytest.fixture()
def app(workdir):
    return make_app(workdir)[0]


@pytest.fixture()
def client(app):
    with TestClient(app) as c:
        yield c


def wait_status(client, batch_id, statuses, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/batches/{batch_id}").json()
        if body["status"] in statuses:
            return body
        time.sleep(0.02)
    raise AssertionError(f"batch never reached {statuses}: {body}")


def read_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


def test_initial_consent_and_subjects(client):
    body = client.get("/subjects").json()
    ids = [s["subject_id"] for s in body["subjects"]]
    assert ids == [f"S{i:03d}" for i in range(1, 9)]
    assert all(s["consented"] for s in body["subjects"])


def test_batch_replaces_subjects_codes_stable_and_order_preserved(app, client, workdir):
    first = client.post("/batches").json()
    done = wait_status(client, first["batch_id"], {"completed"})
    assert done["row_count"] > 0

    csv1 = read_csv(client.get(f"/batches/{first['batch_id']}/download").text)
    header, rows1 = csv1[0], csv1[1:]
    assert header == ["subject_id", "test_date", "test_item", "value"]  # only values change
    assert all(r[0] not in {f"S{i:03d}" for i in range(1, 11)} for r in rows1)

    # Row order and all values other than the subject code are unchanged.
    input_rows = read_csv((workdir / "inputs/lims.csv").read_text())
    in_header, in_rows = input_rows[0], input_rows[1:]
    consented = {f"S{i:03d}" for i in range(1, 9)}
    expected_unmapped = [r[1:] for r in in_rows if r[0] in consented]
    assert [r[1:] for r in rows1] == expected_unmapped

    # A later batch, with a changed consent set, uses identical codes.
    client.put("/subjects/S001/consent", json={"consented": False})

    def code_for(subject_id):
        with app.state.service.db.connect() as conn:
            return conn.execute(
                "SELECT code FROM codes WHERE subject_id=?", (subject_id,)
            ).fetchone()["code"]

    s001_code = code_for("S001")
    second = client.post("/batches").json()
    wait_status(client, second["batch_id"], {"completed"})
    rows2 = read_csv(client.get(f"/batches/{second['batch_id']}/download").text)[1:]
    codes1 = {r[0] for r in rows1}
    codes2 = {r[0] for r in rows2}
    assert codes1 - codes2 == {s001_code}
    for subject_id in (f"S{i:03d}" for i in range(2, 9)):
        assert code_for(subject_id) in codes2
    # Values are unchanged and ordering follows the input extract.
    expected2 = [r[1:] for r in in_rows if r[0] not in {"S001"} and r[0] in consented]
    assert [r[1:] for r in rows2] == expected2


def test_revocation_during_generation_invalidates_and_reconsent_does_not_restore(workdir):
    app, _ = make_app(workdir, row_delay=0.002)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        assert batch["status"] == "running"
        revoked = client.put("/subjects/S003/consent", json={"consented": False}).json()
        assert revoked["changed"] is True
        done = wait_status(client, batch["batch_id"], {"invalidated"}, timeout=15)
        assert done["status"] == "invalidated"
        assert "revoked" in (done["failure_reason"] or "")
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410

        client.put("/subjects/S003/consent", json={"consented": True})
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410
        assert wait_status(client, batch["batch_id"], {"invalidated"})["status"] == "invalidated"


def test_revocation_invalidates_completed_batch(workdir):
    app, _ = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        wait_status(client, batch["batch_id"], {"completed"})
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 200
        client.put("/subjects/S002/consent", json={"consented": False})
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410
        client.put("/subjects/S002/consent", json={"consented": True})
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410


def test_consent_change_during_generation_is_saved_but_does_not_stop_batch(workdir):
    app, _ = make_app(workdir, row_delay=0.002)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        # Revoking an unadmitted subject (S009 was never consented) must save.
        changed = client.put("/subjects/S009/consent", json={"consented": False}).json()
        assert changed["changed"] is True
        assert client.get("/subjects/S009/consent").json()["consented"] is False
        # A grant for a subject not admitted to this batch does not join it.
        client.put("/subjects/S010/consent", json={"consented": True})
        done = wait_status(client, batch["batch_id"], {"completed", "invalidated"})
        assert done["status"] != "invalidated"
        if done["status"] == "completed":
            downloaded = client.get(f"/batches/{batch['batch_id']}/download").text
            assert "S010" not in downloaded


def test_restart_fails_running_batches_and_keeps_intact_completed(workdir):
    app, settings1 = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        completed = client.post("/batches").json()
        wait_status(client, completed["batch_id"], {"completed"})
        assert client.get(f"/batches/{completed['batch_id']}/download").status_code == 200

    app2, settings2 = make_app(workdir, row_delay=10.0)
    with TestClient(app2) as client:
        long_batch = client.post("/batches").json()
        assert long_batch["status"] == "running"

    # Simulate a process restart: brand-new Service over the same data directory.
    app3, _ = make_app(workdir, row_delay=0.0)
    with TestClient(app3) as client:
        revived = client.get(f"/batches/{long_batch['batch_id']}").json()
        assert revived["status"] == "failed"
        assert "restart" in (revived["failure_reason"] or "")
        assert client.get(f"/batches/{long_batch['batch_id']}/download").status_code == 409
        still_ok = client.get(f"/batches/{completed['batch_id']}").json()
        assert still_ok["status"] == "completed"
        assert client.get(f"/batches/{completed['batch_id']}/download").status_code == 200


def test_tampered_file_is_not_served(workdir):
    app, settings = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        wait_status(client, batch["batch_id"], {"completed"})
        files = list((settings.data_dir / "batches").glob("*.csv"))
        assert len(files) == 1
        files[0].write_text("subject_id,test_date,test_item,value\nS001,2025-01-06,X,1\n")
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 409


def test_generation_failure_is_not_downloadable(workdir):
    app, settings = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        batch_dir = settings.data_dir / "batches"
        batch_dir.chmod(0o555)  # worker cannot create its temp file
        try:
            batch = client.post("/batches").json()
            done = wait_status(client, batch["batch_id"], {"failed"}, timeout=15)
        finally:
            batch_dir.chmod(0o755)
        assert "generation error" in (done["failure_reason"] or "")
        assert done["downloadable"] is False
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 409
        assert list(batch_dir.glob("*.csv")) == []


def test_missing_file_on_restart_fails_completed_batch(workdir):
    app, settings = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        wait_status(client, batch["batch_id"], {"completed"})
    for path in (settings.data_dir / "batches").glob("*.csv"):
        path.unlink()
    app2, _ = make_app(workdir, row_delay=0.0)
    with TestClient(app2) as client:
        body = client.get(f"/batches/{batch['batch_id']}").json()
        assert body["status"] == "failed"
        assert "unusable" in (body["failure_reason"] or "")
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 409


def test_unknown_batch_and_invalid_input_contract(client):
    assert client.get("/batches/doesnotexist").status_code == 404
    assert client.get("/batches/doesnotexist/download").status_code == 404


def test_download_in_flight_completes_after_revocation(workdir):
    """A download that has started may finish; subsequent downloads are refused."""
    app, settings = make_app(workdir, row_delay=0.0)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        done = wait_status(client, batch["batch_id"], {"completed"})
        batch_row_count = done["row_count"]
        with client.stream("GET", f"/batches/{batch['batch_id']}/download") as response:
            assert response.status_code == 200
            it = response.iter_bytes()
            first = next(it)  # stream has started
            assert first
            client.put("/subjects/S001/consent", json={"consented": False})
            rest = b"".join(it)
        full = first + rest
        # The in-flight stream is complete and intact, matching its size header.
        assert int(response.headers["content-length"]) == len(full)
        assert full.startswith(b"subject_id,test_date,test_item,value")
        assert full.count(b"\n") == 1 + batch_row_count  # header + 8 subjects x 15 rows
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410
