"""
MODULE 2 - WAL LOG MANAGER                        (Owner: Sujanya Sailwal)
==========================================================================
Write-Ahead Log: an append-only file, one JSON record per line.

 Record fields
   lsn            Log Sequence Number (1,2,3,... never reused)
   txn_id         which transaction wrote it
   type           BEGIN | UPDATE | COMMIT | ABORT | CLR | END | CHECKPOINT
   key,old,new    what changed (old value is needed for UNDO)
   prev_lsn       previous record of the SAME transaction (undo chain)
   undo_next_lsn  (CLR only) next record that still has to be undone

 WAL rule: the log record is forced to disk (flush + fsync) BEFORE the
 changed page may reach the database file.

OS concept   : sequential file I/O, flush/fsync (forcing OS buffers to disk),
               mutex around appends.
DBMS concept : write-ahead logging, LSN, durability.
"""
import json
import os
import re
import threading


class WALManager:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.next_lsn = 1
        self.flushed_lsn = 0                      # highest LSN safely on disk
        if os.path.exists(path):
            records = self.read_all()
            if records:
                self.next_lsn = records[-1]["lsn"] + 1
                self.flushed_lsn = records[-1]["lsn"]
        else:
            open(path, "a").close()

    def append(self, rec_type, txn_id=None, key=None, old=None, new=None,
               prev_lsn=None, undo_next_lsn=None, **extra):
        with self._lock:
            rec = {"lsn": self.next_lsn, "txn_id": txn_id, "type": rec_type,
                   "key": key, "old": old, "new": new,
                   "prev_lsn": prev_lsn, "undo_next_lsn": undo_next_lsn}
            rec.update(extra)
            self.next_lsn += 1
            with open(self.path, "a") as f:
                f.write(json.dumps(rec) + "\n")
                f.flush()
                os.fsync(f.fileno())              # force to physical disk
            self.flushed_lsn = rec["lsn"]
            return rec["lsn"]

    def read_all(self):
        if not os.path.exists(self.path):
            return []
        records = []
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except ValueError:
                    break                          # torn (half-written) last line
        return records

    def record_count(self):
        return len(self.read_all())

    def max_txn_number(self):
        """Highest N among transaction ids T<N> seen in the log."""
        highest = 0
        for r in self.read_all():
            m = re.fullmatch(r"T(\d+)", r.get("txn_id") or "")
            if m:
                highest = max(highest, int(m.group(1)))
        return highest
