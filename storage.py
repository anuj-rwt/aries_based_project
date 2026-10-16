"""
MODULE 1 - STORAGE LAYER                          (Owner: Sujanya Sailwal)
==========================================================================
This is the "disk" of our database: a SQLite file with one table
    kv(key, value, page_lsn)

 * Every key is treated as one "page" of the database.
 * page_lsn = LSN of the last log record that was applied to this page.
   ARIES uses it during REDO to answer: "is this change already on disk?"

OS concept   : file I/O, a mutex so two threads never write the file together.
DBMS concept : persistent storage, page LSN.
"""
import os
import sqlite3
import threading


class Storage:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()          # OS: mutual exclusion on the file
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self._run("CREATE TABLE IF NOT EXISTS kv ("
                  "key TEXT PRIMARY KEY, value TEXT, "
                  "page_lsn INTEGER NOT NULL DEFAULT 0)")

    def _run(self, sql, params=(), fetch=None):
        with self._lock:
            conn = sqlite3.connect(self.path)
            try:
                cur = conn.execute(sql, params)
                if fetch == "one":
                    result = cur.fetchone()
                elif fetch == "all":
                    result = cur.fetchall()
                else:
                    result = None
                conn.commit()
                return result
            finally:
                conn.close()

    def get(self, key):
        """Value of a page as stored on disk (None if it does not exist)."""
        row = self._run("SELECT value FROM kv WHERE key=?", (key,), "one")
        return row[0] if row else None

    def get_page_lsn(self, key):
        row = self._run("SELECT page_lsn FROM kv WHERE key=?", (key,), "one")
        return row[0] if row else 0

    def write_page(self, key, value, lsn):
        """Write one page to disk together with its page LSN."""
        self._run("INSERT INTO kv(key, value, page_lsn) VALUES (?, ?, ?) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                  "page_lsn=excluded.page_lsn", (key, value, lsn))

    def dump(self):
        rows = self._run("SELECT key, value FROM kv ORDER BY key", fetch="all")
        return {k: v for k, v in rows if v is not None}

    def dump_pages(self):
        rows = self._run("SELECT key, value, page_lsn FROM kv ORDER BY key", fetch="all")
        return {k: {"value": v, "page_lsn": l} for k, v, l in rows if v is not None}
