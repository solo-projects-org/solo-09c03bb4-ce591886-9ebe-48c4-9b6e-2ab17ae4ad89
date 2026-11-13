"""Runtime configuration, all overridable via environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser().resolve()


@dataclass(frozen=True)
class Settings:
    # Immutable inputs: the LIMS extract and the initial consent roster.
    input_csv: Path
    initial_consent_csv: Path
    # The one persistent volume: consent state, code registry, batch manifests,
    # generated files and the SQLite database all live under here.
    data_dir: Path
    # Simulated per-row processing cost of a generation worker, in seconds.
    row_delay_seconds: float
    # How often a running worker re-checks whether its batch was invalidated.
    check_interval_rows: int

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = _env_path("DATA_DIR", Path.cwd() / "data")
        return cls(
            input_csv=_env_path("INPUT_CSV", PROJECT_ROOT / "samples/inputs/lims.csv"),
            initial_consent_csv=_env_path(
                "INITIAL_CONSENT_CSV", PROJECT_ROOT / "samples/inputs/initial_consent.csv"
            ),
            data_dir=data_dir,
            row_delay_seconds=float(os.environ.get("ROW_DELAY_SECONDS", "0")),
            check_interval_rows=int(os.environ.get("CHECK_INTERVAL_ROWS", "50")),
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "deid.sqlite3"

    @property
    def batch_dir(self) -> Path:
        return self.data_dir / "batches"
