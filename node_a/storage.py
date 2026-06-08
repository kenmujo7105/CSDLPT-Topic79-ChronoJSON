"""
Storage Engine — Two Versioning Strategies
  FullSnapshotStorage: mỗi version = complete JSON file
  DeltaStorage: v1 = full JSON, v2+ = JSON Patch (RFC 6902)
"""

import json
import os
import copy
import uuid
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

import jsonpatch


# ============================================================================
# Base mixin for shared SQLite + file helpers
# ============================================================================

class _StorageBase:
    """Shared utilities for both strategies."""

    def __init__(self, db_path: str, data_dir: str, table_prefix: str):
        self.db_path = db_path
        self.data_dir = data_dir
        self.table_prefix = table_prefix
        self._lock = threading.Lock()
        os.makedirs(data_dir, exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.executescript(self._schema_sql())
        conn.commit()
        conn.close()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        return c

    def _schema_sql(self) -> str:
        raise NotImplementedError

    def _write_json_file(self, subdir: str, doc_id: str, version: int, data: dict | list) -> tuple[str, int]:
        """Write JSON to file, return (path, size_bytes)."""
        doc_dir = os.path.join(self.data_dir, subdir, doc_id)
        os.makedirs(doc_dir, exist_ok=True)
        path = os.path.join(doc_dir, f"v{version:04d}.json")
        raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        with open(path, "wb") as f:
            f.write(raw)
        return path, len(raw)

    def _read_json_file(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


# ============================================================================
# Strategy A: Full Snapshot Storage
# ============================================================================

class FullSnapshotStorage(_StorageBase):
    """
    Every version stores the COMPLETE document content as a JSON file.
    Time-travel = direct O(1) lookup by timestamp in SQLite index.
    """

    def __init__(self, db_path: str, data_dir: str):
        super().__init__(db_path, data_dir, "snapshots")

    def _schema_sql(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id     TEXT PRIMARY KEY,
            title      TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id         TEXT PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            version    INTEGER NOT NULL,
            timestamp  TEXT NOT NULL,
            author_id  TEXT NOT NULL,
            file_path  TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            UNIQUE(doc_id, version)
        );
        CREATE INDEX IF NOT EXISTS idx_snap_doc_ts
            ON snapshots(doc_id, timestamp);
        """

    def save_version(self, doc_id: str, content: dict, timestamp: str, author: str,
                     title: str = None) -> dict:
        """Save a complete snapshot of the document."""
        with self._lock:
            conn = self._conn()
            try:
                # Auto-register doc
                conn.execute(
                    "INSERT OR IGNORE INTO documents (doc_id, title, created_at) VALUES (?,?,?)",
                    (doc_id, title or content.get("title", doc_id), timestamp),
                )
                # Next version
                row = conn.execute(
                    "SELECT COALESCE(MAX(version),0) as v FROM snapshots WHERE doc_id=?",
                    (doc_id,),
                ).fetchone()
                version = row["v"] + 1

                # Write JSON file
                path, size = self._write_json_file("snapshots", doc_id, version, content)

                # Index in SQLite
                sid = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO snapshots (id,doc_id,version,timestamp,author_id,file_path,size_bytes) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (sid, doc_id, version, timestamp, author, path, size),
                )
                conn.commit()
                return {"version": version, "size_bytes": size, "timestamp": timestamp}
            finally:
                conn.close()

    def get_at_time(self, doc_id: str, timestamp: str) -> Optional[dict]:
        """Time-Travel Query: O(1) — find latest snapshot <= timestamp."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM snapshots WHERE doc_id=? AND timestamp<=? "
                "ORDER BY timestamp DESC LIMIT 1",
                (doc_id, timestamp),
            ).fetchone()
            if not row:
                return None
            content = self._read_json_file(row["file_path"])
            return {
                "doc_id": doc_id,
                "version": row["version"],
                "timestamp": row["timestamp"],
                "author_id": row["author_id"],
                "content": content,
            }
        finally:
            conn.close()

    def get_latest(self, doc_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM snapshots WHERE doc_id=? ORDER BY version DESC LIMIT 1",
                (doc_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "doc_id": doc_id,
                "version": row["version"],
                "timestamp": row["timestamp"],
                "content": self._read_json_file(row["file_path"]),
            }
        finally:
            conn.close()

    def get_history(self, doc_id: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT version, timestamp, author_id, size_bytes "
                "FROM snapshots WHERE doc_id=? ORDER BY version", (doc_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_storage_size(self) -> int:
        """Total bytes of all snapshot JSON files."""
        conn = self._conn()
        try:
            row = conn.execute("SELECT COALESCE(SUM(size_bytes),0) as total FROM snapshots").fetchone()
            return row["total"]
        finally:
            conn.close()

    def get_version_count(self) -> int:
        conn = self._conn()
        try:
            return conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
        finally:
            conn.close()


# ============================================================================
# Strategy B: Delta Encoding Storage
# ============================================================================

class DeltaStorage(_StorageBase):
    """
    v1 = full JSON snapshot (base). v2+ = JSON Patch (RFC 6902 delta).
    Time-travel = replay patches from base to target timestamp: O(n).
    """

    def __init__(self, db_path: str, data_dir: str):
        super().__init__(db_path, data_dir, "deltas")

    def _schema_sql(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id     TEXT PRIMARY KEY,
            title      TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS deltas (
            id           TEXT PRIMARY KEY,
            doc_id       TEXT NOT NULL,
            version      INTEGER NOT NULL,
            timestamp    TEXT NOT NULL,
            author_id    TEXT NOT NULL,
            is_base      INTEGER NOT NULL DEFAULT 0,
            base_version INTEGER,
            file_path    TEXT NOT NULL,
            size_bytes   INTEGER NOT NULL,
            UNIQUE(doc_id, version)
        );
        CREATE INDEX IF NOT EXISTS idx_delta_doc_ts
            ON deltas(doc_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_delta_doc_ver
            ON deltas(doc_id, version);
        """

    def save_version(self, doc_id: str, content: dict, timestamp: str, author: str,
                     title: str = None, previous_content: dict = None) -> dict:
        """
        Save version. Auto-computes delta patch vs previous_content.
        If version==1 or no previous, stores full content as base.
        """
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO documents (doc_id, title, created_at) VALUES (?,?,?)",
                    (doc_id, title or content.get("title", doc_id), timestamp),
                )
                row = conn.execute(
                    "SELECT COALESCE(MAX(version),0) as v FROM deltas WHERE doc_id=?",
                    (doc_id,),
                ).fetchone()
                version = row["v"] + 1

                is_base = (version == 1 or previous_content is None)

                if is_base:
                    file_data = content
                else:
                    patch = jsonpatch.make_patch(previous_content, content)
                    file_data = patch.patch  # list of RFC 6902 ops

                path, size = self._write_json_file("deltas", doc_id, version, file_data)

                did = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO deltas (id,doc_id,version,timestamp,author_id,"
                    "is_base,base_version,file_path,size_bytes) VALUES (?,?,?,?,?,?,?,?,?)",
                    (did, doc_id, version, timestamp, author,
                     1 if is_base else 0,
                     None if is_base else version - 1,
                     path, size),
                )
                conn.commit()
                return {
                    "version": version, "size_bytes": size,
                    "timestamp": timestamp, "is_base": is_base,
                }
            finally:
                conn.close()

    def get_at_time(self, doc_id: str, timestamp: str) -> Optional[dict]:
        """
        Time-Travel Query: reconstruct document by replaying deltas.
        Returns content + number of patches applied (for benchmarking).
        """
        conn = self._conn()
        try:
            # Get base version
            base = conn.execute(
                "SELECT * FROM deltas WHERE doc_id=? AND is_base=1 ORDER BY version LIMIT 1",
                (doc_id,),
            ).fetchone()
            if not base or base["timestamp"] > timestamp:
                return None

            current = self._read_json_file(base["file_path"])

            # Get all patches between base and target timestamp
            patches = conn.execute(
                "SELECT * FROM deltas WHERE doc_id=? AND is_base=0 "
                "AND version>? AND timestamp<=? ORDER BY version ASC",
                (doc_id, base["version"], timestamp),
            ).fetchall()

            patches_applied = 0
            last_ver = base["version"]
            last_ts = base["timestamp"]
            last_author = base["author_id"]

            for p in patches:
                ops = self._read_json_file(p["file_path"])
                if ops:
                    patch = jsonpatch.JsonPatch(ops)
                    current = patch.apply(current)
                patches_applied += 1
                last_ver = p["version"]
                last_ts = p["timestamp"]
                last_author = p["author_id"]

            return {
                "doc_id": doc_id,
                "version": last_ver,
                "timestamp": last_ts,
                "author_id": last_author,
                "content": current,
                "patches_applied": patches_applied,
            }
        finally:
            conn.close()

    def get_latest(self, doc_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT MAX(timestamp) as ts FROM deltas WHERE doc_id=?", (doc_id,),
            ).fetchone()
            if not row or not row["ts"]:
                return None
            return self.get_at_time(doc_id, row["ts"])
        finally:
            conn.close()

    def get_history(self, doc_id: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT version, timestamp, author_id, size_bytes, is_base "
                "FROM deltas WHERE doc_id=? ORDER BY version", (doc_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_storage_size(self) -> int:
        """Total bytes of all delta JSON files."""
        conn = self._conn()
        try:
            return conn.execute(
                "SELECT COALESCE(SUM(size_bytes),0) as total FROM deltas"
            ).fetchone()["total"]
        finally:
            conn.close()

    def get_version_count(self) -> int:
        conn = self._conn()
        try:
            return conn.execute("SELECT COUNT(*) as c FROM deltas").fetchone()["c"]
        finally:
            conn.close()
