"""SQLite-based message queue for inter-service communication."""
import json
from datetime import datetime
from typing import Any, Dict, Optional
import sqlite3
from pathlib import Path


class MessageQueue:
    """Simple SQLite-based message queue."""

    def __init__(self, db_path: str = "data/queue.db"):
        """Initialize the message queue."""
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database table."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS message_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def enqueue(self, message_type: str, payload: Dict[str, Any]) -> int:
        """Add a message to the queue."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "INSERT INTO message_queue (message_type, payload) VALUES (?, ?)",
                (message_type, json.dumps(payload))
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def dequeue(self, message_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get and mark a message as processing."""
        conn = sqlite3.connect(self.db_path)
        try:
            if message_type:
                cursor = conn.execute(
                    """SELECT id, message_type, payload FROM message_queue
                       WHERE status = 'pending' AND message_type = ?
                       ORDER BY created_at LIMIT 1""",
                    (message_type,)
                )
            else:
                cursor = conn.execute(
                    """SELECT id, message_type, payload FROM message_queue
                       WHERE status = 'pending'
                       ORDER BY created_at LIMIT 1"""
                )

            row = cursor.fetchone()
            if row:
                conn.execute(
                    "UPDATE message_queue SET status = 'processing' WHERE id = ?",
                    (row[0],)
                )
                conn.commit()
                return {
                    "id": row[0],
                    "type": row[1],
                    "payload": json.loads(row[2])
                }
            return None
        finally:
            conn.close()

    def complete(self, message_id: int) -> None:
        """Mark a message as completed."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """UPDATE message_queue
                   SET status = 'completed', processed_at = ?
                   WHERE id = ?""",
                (datetime.utcnow().isoformat(), message_id)
            )
            conn.commit()
        finally:
            conn.close()