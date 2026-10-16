"""
MODULE 9 - ENGINE (integration)                   (Owner: Anuj Rawat)
==========================================================================
Wires all modules together into one database system and gives the rest of
the program (REST API, scheduler, scenarios, tests) ONE simple interface.

   Storage <- BufferManager <- TransactionManager -> LockManager
                  ^                    |
                  |                    v
              WALManager <------- CrashSimulator
                  ^
                  +------ Recovery (ARIES)

 crash()   : wipe everything that lives only in memory
 recover() : run ARIES using the log and database file that survived
 If the program is restarted and a log already exists, recovery runs
 automatically at start-up - just like a real DBMS.
"""
import os
import time

import recovery
from buffer_manager import BufferManager
from crash_simulator import CRASH_POINTS, CrashSimulator, SimulatedCrash
from lock_manager import DeadlockError, LockManager, LockTimeout
from storage import Storage
from transaction_manager import TransactionError, TransactionManager
from wal_manager import WALManager


class Engine:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.db_path = os.path.join(data_dir, "database.db")
        self.wal_path = os.path.join(data_dir, "wal.log")
        self.running = True
        self.last_recovery = None
        self._build()
        if self.wal.record_count() > 0:             # restart after a crash
            self.last_recovery = self.recover()

    def _build(self):
        self.storage = Storage(self.db_path)
        self.wal = WALManager(self.wal_path)
        self.buffer = BufferManager(self.storage, self.wal)
        self.locks = LockManager()
        self.crash_sim = CrashSimulator()
        self.tm = TransactionManager(self.wal, self.buffer, self.locks, self.crash_sim)

    def reset(self):
        for p in (self.db_path, self.wal_path):
            if os.path.exists(p):
                os.remove(p)
        self._build()
        self.running = True
        self.last_recovery = None

    # ------------------------------------------------------------ guard wrapper
    def _guard(self, txn_id, fn):
        if not self.running:
            return {"status": "ERROR", "message": "System is DOWN (crashed). Run recovery first."}
        try:
            return fn()
        except SimulatedCrash as c:
            self.crash()
            return {"status": "CRASHED", "point": c.point, "message": CRASH_POINTS[c.point]}
        except DeadlockError as e:
            return {"status": "DEADLOCK", "cycle": e.cycle, "victim": txn_id,
                    "message": f"{txn_id} chosen as victim: rolled back and locks released"}
        except LockTimeout as e:
            return {"status": "TIMEOUT", "message": str(e)}
        except TransactionError as e:
            return {"status": "ERROR", "message": str(e)}

    # --------------------------------------------------------- transaction API
    def begin(self):
        if not self.running:
            return {"status": "ERROR", "message": "System is DOWN (crashed). Run recovery first."}
        return {"status": "OK", "txn_id": self.tm.begin()}

    def write(self, txn_id, key, value, blocking=True, timeout=5.0):
        return self._guard(txn_id, lambda: self.tm.write(txn_id, key, value, blocking, timeout))

    def read(self, txn_id, key, blocking=True, timeout=5.0):
        return self._guard(txn_id, lambda: self.tm.read(txn_id, key, blocking, timeout))

    def commit(self, txn_id):
        return self._guard(txn_id, lambda: self.tm.commit(txn_id))

    def abort(self, txn_id):
        return self._guard(txn_id, lambda: self.tm.abort(txn_id))

    def execute(self, txn_id, op, blocking=True, timeout=5.0):
        kind = op[0]
        if kind == "write":
            return self.write(txn_id, op[1], op[2], blocking, timeout)
        if kind == "read":
            return self.read(txn_id, op[1], blocking, timeout)
        if kind == "commit":
            return self.commit(txn_id)
        if kind == "abort":
            return self.abort(txn_id)
        if kind == "sleep":
            time.sleep(float(op[1]))
            return {"status": "OK"}
        return {"status": "ERROR", "message": f"unknown operation {kind}"}

    # ------------------------------------------------- buffer / crash / recovery
    def flush(self):
        if not self.running:
            return {"status": "ERROR", "message": "System is DOWN"}
        return {"status": "OK", "flushed": self.buffer.flush_all()}

    def checkpoint(self):
        if not self.running:
            return {"status": "ERROR", "message": "System is DOWN"}
        return {"status": "OK", **self.tm.checkpoint()}

    def arm_crash(self, point):
        self.crash_sim.arm(point)

    def crash(self):
        """Power failure: memory is lost, files on disk survive."""
        self.running = False
        self.buffer.clear()
        self.locks.clear()
        self.tm.reset()
        self.crash_sim.disarm()

    def recover(self):
        self.last_recovery = recovery.recover(self.wal, self.buffer)
        self.tm.reset()
        self.running = True
        return self.last_recovery

    # ----------------------------------------------------------------- views
    def visible_database(self):
        """What a reader would see now: disk pages overlaid with buffered pages."""
        db = self.storage.dump()
        for k, p in self.buffer.snapshot().items():
            if p["value"] is None:
                db.pop(k, None)
            else:
                db[k] = p["value"]
        return db

    def state(self):
        lock_state = self.locks.snapshot()
        return {
            "running": self.running,
            "armed_crash": self.crash_sim.armed,
            "transactions": self.tm.snapshot(),
            "locks": lock_state["locks"],
            "waits_for": lock_state["waits_for"],
            "buffer": self.buffer.snapshot(),
            "disk": self.storage.dump_pages(),
            "last_lsn": self.wal.next_lsn - 1,
        }

    def wal_records(self):
        return self.wal.read_all()
