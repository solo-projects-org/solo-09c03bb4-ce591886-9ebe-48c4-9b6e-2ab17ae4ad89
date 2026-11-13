#!/usr/bin/env python3
"""End-to-end demonstration against a running API (default http://localhost:8000).

Shows, in order:
  1. a batch generating, revoked mid-flight     -> invalidated, download 410
  2. re-consent of that subject                 -> old batch still 410
  3. a fresh batch after re-consent             -> completed, downloads fine
  4. revoking a subject of the completed batch -> that batch 410 as well
Requires ROW_DELAY_SECONDS > 0 (docker-compose sets 0.02) so step 1 is mid-flight.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import urllib.error
import urllib.request

BASE = "http://localhost:8000"
REVOKE_SUBJECT = "S003"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            ctype = resp.headers.get("content-type", "")
            return resp.status, (json.loads(raw) if "json" in ctype else raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def wait(batch_id: str, terminal: set[str], timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = call("GET", f"/batches/{batch_id}")
        if status == 200 and body["status"] in terminal:
            return body
        time.sleep(0.1)
    raise SystemExit(f"batch {batch_id} did not reach {terminal}")


def show(label: str, status: int, body: object) -> None:
    if isinstance(body, bytes):
        preview = body.decode(errors="replace").splitlines()[:3]
        body = {"csv_head": preview, "bytes": len(body)}
    print(f"{label}: HTTP {status} {json.dumps(body, ensure_ascii=False)}")


def main() -> int:
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=BASE)
    args = parser.parse_args()
    BASE = args.base

    print("== initial consent roster ==")
    _, subjects = call("GET", "/subjects")
    print(json.dumps(subjects, ensure_ascii=False, indent=2)[:1200])

    print(f"\n== 1. start batch, revoke {REVOKE_SUBJECT} while it is generating ==")
    status, batch = call("POST", "/batches")
    show("POST /batches", status, batch)
    time.sleep(0.4)  # let the worker get into the extract
    _, revoked = call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": False})
    show(f"PUT /subjects/{REVOKE_SUBJECT}/consent (revoke)", 200, revoked)
    final = wait(batch["batch_id"], {"invalidated", "completed", "failed"})
    show("batch after revocation", 200, final)
    status, body = call("GET", f"/batches/{batch['batch_id']}/download")
    show("download invalidated batch", status, body)

    print(f"\n== 2. re-consent {REVOKE_SUBJECT}; the old batch must stay dead ==")
    _, restored = call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": True})
    show("PUT consent (re-grant)", 200, restored)
    status, body = call("GET", f"/batches/{batch['batch_id']}/download")
    show("download old batch after re-consent", status, body)

    print("\n== 3. new batch after re-consent generates normally ==")
    status, batch2 = call("POST", "/batches")
    show("POST /batches", status, batch2)
    done = wait(batch2["batch_id"], {"completed", "failed", "invalidated"})
    show("batch result", 200, done)
    status, body = call("GET", f"/batches/{batch2['batch_id']}/download")
    show("download new batch", status, body)

    print(f"\n== 4. revoke again: completed batch {batch2['batch_id'][:8]} is also invalidated ==")
    _, revoked2 = call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": False})
    show("PUT consent (revoke)", 200, revoked2)
    status, body = call("GET", f"/batches/{batch2['batch_id']}/download")
    show("download completed batch after revocation", status, body)

    _, after = call("GET", f"/batches/{batch2['batch_id']}")
    print("\nfinal batch registry:", json.dumps(after, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
