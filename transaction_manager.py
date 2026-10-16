"""
MODULE 6 - TRANSACTION MANAGER                    (Owner: Anuj Rawat)
==========================================================================
Runs transactions: begin / read / write / commit / abort / checkpoint.

 Rules it enforces
   1. Lock the key first (Lock Manager)           -> isolation
   2. Write the WAL record BEFORE changing the page -> write-ahead logging
   3. Change the page only in the buffer (no-force) -> data reaches disk
      later (flush / checkpoint), so REDO is really needed after a crash
   4. COMMIT = a COMMIT record forced to the log. Nothing else is needed
      for durability.
   5. ABORT = undo the transaction's updates using the log, writing a CLR
      (compensation record) for each undone update.

OS concept   : each transaction is one unit of execution (thread/process);
               a mutex protects the shared tables.
DBMS concept : transactions, ACID, WAL, commit/rollback, checkpoint.
"""
import threading

from lock_manager import BLOCKED, DeadlockError, LockTimeout


class TransactionError(Exception):
    pass


class TransactionManager:
    def __init__(self, wal, buffer, locks, crash):
        self.wal = wal
        self.buffer = buffer
        self.locks = locks
        self.crash = crash
        self.txns = {}                          # txn_id -> {"status", "last_lsn"}
        self._mutex = threading.RLock()
        self._counter = wal.max_txn_number()

    def reset(self):
        """Called after a crash: the in-memory transaction table is gone."""
        with self._mutex:
            self.txns.clear()
            self._counter = self.wal.max_txn_number()

    def snapshot(self):
        with self._mutex:
            return {t: dict(i) for t, i in self.txns.items()}

    def _active(self, txn_id):
        txn = self.txns.get(txn_id)
        if txn is None or txn["status"] != "ACTIVE":
            raise TransactionError(f"{txn_id} is not an active transaction")
        return txn

    def _lock(self, txn_id, key, blocking, timeout):
        try:
            return self.locks.acquire(txn_id, key, blocking, timeout)
        except (DeadlockError, LockTimeout):
            self.abort(txn_id)                  # victim is rolled back
            raise

    # ------------------------------------------------------------------ BEGIN
    def begin(self):
        with self._mutex:
            self._counter += 1
            tid = f"T{self._counter}"
            lsn = self.wal.append("BEGIN", tid)
            self.txns[tid] = {"status": "ACTIVE", "last_lsn": lsn}
            return tid

    # ------------------------------------------------------------------- READ
    def read(self, txn_id, key, blocking=True, timeout=5.0):
        self._active(txn_id)
        if self._lock(txn_id, key, blocking, timeout) == BLOCKED:
            return {"status": "BLOCKED", "waiting_for": self.locks.holder(key)}
        return {"status": "OK", "value": self.buffer.read(key)}

    # ------------------------------------------------------------------ WRITE
    def write(self, txn_id, key, value, blocking=True, timeout=5.0):
        txn = self._active(txn_id)
        self.crash.check("BEFORE_WAL")
        if self._lock(txn_id, key, blocking, timeout) == BLOCKED:
            return {"status": "BLOCKED", "waiting_for": self.locks.holder(key)}
        with self._mutex:
            old = self.buffer.read(key)
            lsn = self.wal.append("UPDATE", txn_id, key, old, str(value),
                                  prev_lsn=txn["last_lsn"])          # 1) log first
            txn["last_lsn"] = lsn
            self.crash.check("AFTER_WAL_BEFORE_UPDATE")
            self.buffer.put(key, str(value), lsn)                    # 2) then page
            self.crash.check("AFTER_UPDATE_BEFORE_COMMIT",
                             pre_crash=lambda: self.buffer.flush_page(key))
            return {"status": "OK", "lsn": lsn}

    # ----------------------------------------------------------------- COMMIT
    def commit(self, txn_id):
        with self._mutex:
            txn = self._active(txn_id)
            lsn = self.wal.append("COMMIT", txn_id, prev_lsn=txn["last_lsn"])
            txn["last_lsn"] = lsn
            txn["status"] = "COMMITTED"
            self.crash.check("AFTER_COMMIT")
            self.locks.release_all(txn_id)
            self.wal.append("END", txn_id, prev_lsn=lsn)
            return {"status": "COMMITTED", "lsn": lsn}

    # ------------------------------------------------------------------ ABORT
    def abort(self, txn_id):
        with self._mutex:
            txn = self._active(txn_id)
            lsn = self.wal.append("ABORT", txn_id, prev_lsn=txn["last_lsn"])
            txn["last_lsn"] = lsn
            by_lsn = {r["lsn"]: r for r in self.wal.read_all()}
            cur = lsn
            while cur is not None:                  # walk the undo chain backwards
                rec = by_lsn[cur]
                if rec["type"] == "UPDATE":
                    clr = self.wal.append("CLR", txn_id, rec["key"],
                                          old=rec["new"], new=rec["old"],
                                          prev_lsn=txn["last_lsn"],
                                          undo_next_lsn=rec["prev_lsn"])
                    txn["last_lsn"] = clr
                    self.buffer.put(rec["key"], rec["old"], clr)
                cur = rec["prev_lsn"]
            self.wal.append("END", txn_id, prev_lsn=txn["last_lsn"])
            txn["status"] = "ABORTED"
            self.locks.release_all(txn_id)
            return {"status": "ABORTED"}

    # ------------------------------------------------------------- CHECKPOINT
    def checkpoint(self):
        """Flush all dirty pages, then log the list of active transactions."""
        with self._mutex:
            active = {t: i["last_lsn"] for t, i in self.txns.items()
                      if i["status"] == "ACTIVE"}
            flushed = self.buffer.flush_all()
            lsn = self.wal.append("CHECKPOINT", txn_table=active,
                                  dpt=self.buffer.dirty_page_table())
            return {"lsn": lsn, "active": active, "flushed_pages": flushed}
