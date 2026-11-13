"""Cross-batch-stable random study codes.

Codes are derived from keyed HMAC-SHA256 over the subject id with a random key
held in the database, so the mapping is deterministic (and therefore stable
across batches) for a given subject but not derivable from the subject id alone.
A code is only ever minted inside the transaction that admits the subject to
a batch, i.e. only for someone consented at that batch's start.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import string

CODE_LEN = 8
# Crockford-ish alphabet without ambiguous characters, suitable for headers.
ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"


class CodeRegistry:
    def __init__(self, db) -> None:
        self._db = db
        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('code_key', ?) "
                "ON CONFLICT(key) DO NOTHING",
                (secrets.token_bytes(32).hex(),),
            )
            row = conn.execute("SELECT value FROM meta WHERE key='code_key'").fetchone()
        self._key = bytes.fromhex(row["value"])

    def _candidate(self, subject_id: str, salt: int) -> str:
        material = subject_id if salt == 0 else f"{subject_id}\x00{salt}"
        digest = hmac.new(self._key, material.encode("utf-8"), hashlib.sha256).digest()
        # 8 bytes -> 8 symbols over a 32-symbol alphabet, i.e. 40 bits.
        return "".join(ALPHABET[b % len(ALPHABET)] for b in digest[:CODE_LEN])

    def get_or_create(self, conn, subject_id: str, now: str) -> str:
        """Return the stable code for a subject, minting it if first seen.

        Callers must run this inside the transaction that admits the subject to
        a batch so a code is never created for a non-consenting subject.
        """
        row = conn.execute(
            "SELECT code FROM codes WHERE subject_id=?", (subject_id,)
        ).fetchone()
        if row:
            return row["code"]
        salt = 0
        while True:
            candidate = self._candidate(subject_id, salt)
            if conn.execute("SELECT 1 FROM codes WHERE code=?", (candidate,)).fetchone() is None:
                break
            salt += 1
        conn.execute(
            "INSERT INTO codes(subject_id, code, created_at) VALUES(?,?,?)",
            (subject_id, candidate, now),
        )
        return candidate
