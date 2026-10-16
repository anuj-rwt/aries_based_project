"""
MODULE 3 - BUFFER MANAGER                         (Owner: Sujanya Sailwal)
==========================================================================
An in-memory page cache that sits between transactions and the disk.

 * Updates go to the buffer first (fast, but VOLATILE - lost in a crash).
 * A page in the buffer that differs from disk is a "dirty page".
 * rec_lsn = LSN of the FIRST update that dirtied the page. The smallest
   rec_lsn tells recovery where REDO has to start.
 * flush_page() enforces the WAL rule: the log must already be on disk up
   to the page's LSN before the page itself is written.

OS concept   : page cache / buffering, volatile memory vs. disk.
DBMS concept : dirty page table, steal/no-force buffer policy, WAL rule.
"""
import threading


class BufferManager:
    def __init__(self, storage, wal):
        self.storage = storage
        self.wal = wal
        self._pages = {}                  # key -> {"value", "page_lsn", "rec_lsn"}
        self._lock = threading.RLock()

    def read(self, key):
        with self._lock:
            if key in self._pages:
                return self._pages[key]["value"]
            return self.storage.get(key)

    def page_lsn(self, key):
        with self._lock:
            if key in self._pages:
                return self._pages[key]["page_lsn"]
            return self.storage.get_page_lsn(key)

    def put(self, key, value, lsn):
        """Change a page IN MEMORY only (page becomes dirty)."""
        with self._lock:
            page = self._pages.get(key)
            if page is None:
                self._pages[key] = {"value": value, "page_lsn": lsn, "rec_lsn": lsn}
            else:
                page["value"] = value
                page["page_lsn"] = lsn

    def flush_page(self, key):
        with self._lock:
            page = self._pages.get(key)
            if page is None:
                return False
            assert self.wal.flushed_lsn >= page["page_lsn"], "WAL rule violated!"
            self.storage.write_page(key, page["value"], page["page_lsn"])
            del self._pages[key]
            return True

    def flush_all(self):
        with self._lock:
            keys = list(self._pages.keys())
            for k in keys:
                self.flush_page(k)
            return keys

    def dirty_page_table(self):
        with self._lock:
            return {k: p["rec_lsn"] for k, p in self._pages.items()}

    def snapshot(self):
        with self._lock:
            return {k: dict(p) for k, p in self._pages.items()}

    def clear(self):
        """A crash: everything in memory disappears."""
        with self._lock:
            self._pages.clear()
