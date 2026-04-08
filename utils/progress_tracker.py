"""Progress tracker — persists migration state to a local SQLite database."""
import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

DB_PATH = "/tmp/migration_state.db"


class ProgressTracker:
    """Tracks per-file migration status in SQLite for idempotent reruns."""

    def __init__(self, run_id: str, db_path: str = DB_PATH):
        self.run_id = run_id
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS migration_progress (
                    run_id      TEXT NOT NULL,
                    hdfs_path   TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    rows_count  INTEGER DEFAULT 0,
                    updated_at  TEXT NOT NULL,
                    PRIMARY KEY (run_id, hdfs_path)
                )
            """)
            conn.commit()

    def update(self, hdfs_path: str, status: str, rows_count: int = 0):
        """Insert or update the migration status for a path."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO migration_progress (run_id, hdfs_path, status, rows_count, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, hdfs_path) DO UPDATE SET
                    status=excluded.status,
                    rows_count=excluded.rows_count,
                    updated_at=excluded.updated_at
            """, (self.run_id, hdfs_path, status, rows_count, datetime.utcnow().isoformat()))

    def get_status(self, hdfs_path: str) -> Optional[str]:
        """Return the current migration status for a path, or None if not started."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT status FROM migration_progress WHERE run_id=? AND hdfs_path=?",
                (self.run_id, hdfs_path)
            ).fetchone()
        return row[0] if row else None

    def get_summary(self) -> Dict[str, int]:
        """Return a count summary of statuses for this run."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as cnt, SUM(rows_count) as total_rows "
                "FROM migration_progress WHERE run_id=? GROUP BY status",
                (self.run_id,)
            ).fetchall()
        return {row[0]: {"count": row[1], "total_rows": row[2] or 0} for row in rows}
