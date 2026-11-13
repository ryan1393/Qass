"""Persistent stroke state (serial, stroke counter, in-progress buffer) in SQLite."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any


class StrokeStateStore:
    """Single-row SQLite store so a restart continues the stroke counter."""

    def __init__(self, path: str | os.PathLike = "state.db") -> None:
        """Open (or create) the database; a corrupt file is deleted and recreated."""
        self.path = path
        try:
            self.conn = sqlite3.connect(
                self.path,
                check_same_thread=False,  # REQUIRED for asyncio + Windows
                timeout=30,
            )
            self._init_db()
        except sqlite3.DatabaseError:
            logging.error("State DB corrupt – recreating")
            if os.path.exists(self.path):
                os.remove(self.path)
            self.conn = sqlite3.connect(self.path)
            self._init_db()

    def _init_db(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stroke_state (
                id INTEGER PRIMARY KEY,
                ident TEXT,
                stroke INTEGER,
                buffer TEXT
            )
        """)
        self.conn.commit()

    def load(self) -> tuple[Any, int, list[dict]]:
        """Return (serial, stroke, buffer); (None, 1, []) when nothing is stored."""
        cur = self.conn.execute(
            "SELECT ident, stroke, buffer FROM stroke_state WHERE id = 1"
        )
        row = cur.fetchone()
        if not row:
            return None, 1, []
        ident, stroke, buffer = row
        return ident, stroke, json.loads(buffer) if buffer else []

    def save(self, ident: Any, stroke: int, buffer: list[dict]) -> None:
        """Replace the stored state."""
        self.conn.execute("DELETE FROM stroke_state")
        self.conn.execute(
            "INSERT INTO stroke_state VALUES (1, ?, ?, ?)",
            (ident, stroke, json.dumps(buffer)),
        )
        self.conn.commit()
