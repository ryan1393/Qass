# Manages persistent stroke state using SQLite

import sqlite3
import os
import json
import logging

class StrokeStateStore:
    # Initialize or recover the SQLite database
    def __init__(self, path="state.db"):
        self.path = path
        try:            
            self.conn = sqlite3.connect(
                self.path,
                check_same_thread=False,  # REQUIRED for asyncio + Windows
                timeout=30
            )
            self._init_db()
        except sqlite3.DatabaseError:
            logging.error("State DB corrupt – recreating")
            if os.path.exists(self.path):
                os.remove(self.path)
            self.conn = sqlite3.connect(self.path)
            self._init_db()

    def _init_db(self):
        # Create table if needed
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stroke_state (
                id INTEGER PRIMARY KEY,
                ident TEXT,
                stroke INTEGER,
                buffer TEXT
            )
        """)
        self.conn.commit()

    def load(self):
        # Load last known state
        cur = self.conn.execute(
            "SELECT ident, stroke, buffer FROM stroke_state WHERE id = 1"
        )
        row = cur.fetchone()
        if not row:
            return None, 1, []
        ident, stroke, buffer = row
        return ident, stroke, json.loads(buffer) if buffer else []

    def save(self, ident, stroke, buffer):
        # Persist current state
        self.conn.execute("DELETE FROM stroke_state")
        self.conn.execute(
            "INSERT INTO stroke_state VALUES (1, ?, ?, ?)",
            (ident, stroke, json.dumps(buffer)),
        )
        self.conn.commit()
