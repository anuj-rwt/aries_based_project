"""
MODULE 4 - LOCK MANAGER                           (Owner: Abhay Singh)
==========================================================================
Concurrency control using exclusive locks (strict two-phase locking):
  * a transaction must lock a key before reading or writing it
  * it keeps all its locks until COMMIT / ABORT, then releases them together

Deadlock handling uses a WAIT-FOR GRAPH:
  edge  T1 -> T2   means "T1 is waiting for a lock that T2 holds".
  A cycle in this graph (T1 -> T2 -> T1) is a deadlock. We check for a cycle
  every time a transaction has to wait; the requester becomes the victim.

OS concept   : mutual exclusion, condition variable (threads sleep while
               waiting for a lock), deadlock detection, timeouts.
DBMS concept : isolation (ACID "I"), strict 2PL.
"""
import threading
import time

GRANTED = "GRANTED"
BLOCKED = "BLOCKED"


class DeadlockError(Exception):
    def __init__(self, cycle):
        self.cycle = cycle
        super().__init__("Deadlock: " + " -> ".join(cycle))


class LockTimeout(Exception):
    pass


class LockManager:
    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self.locks = {}             # key -> txn_id holding the lock
        self.waits_for = {}         # waiting txn -> txn it waits for

    def holder(self, key):
        return self.locks.get(key)

    def acquire(self, txn_id, key, blocking=True, timeout=5.0):
        """
        blocking=True  : thread sleeps until the lock is free (real threads)
        blocking=False : returns BLOCKED immediately (used by the scheduler)
        """
        deadline = time.time() + timeout
        with self._cond:
            while True:
                holder = self.locks.get(key)
                if holder is None or holder == txn_id:
                    self.locks[key] = txn_id
                    self.waits_for.pop(txn_id, None)
                    return GRANTED
                # somebody else holds it -> we must wait for them
                self.waits_for[txn_id] = holder
                cycle = self._find_cycle(txn_id)
                if cycle:
                    self.waits_for.pop(txn_id, None)
                    raise DeadlockError(cycle)
                if not blocking:
                    return BLOCKED
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cond.wait(remaining):
                    self.waits_for.pop(txn_id, None)
                    raise LockTimeout(f"{txn_id} timed out waiting for '{key}' held by {holder}")

    def release_all(self, txn_id):
        with self._cond:
            for k in [k for k, v in self.locks.items() if v == txn_id]:
                del self.locks[k]
            self.waits_for.pop(txn_id, None)
            for w in [w for w, h in self.waits_for.items() if h == txn_id]:
                del self.waits_for[w]
            self._cond.notify_all()               # wake sleeping threads

    def _find_cycle(self, start):
        path = [start]
        node = self.waits_for.get(start)
        while node is not None:
            if node == start:
                return path + [start]
            if node in path:
                return None
            path.append(node)
            node = self.waits_for.get(node)
        return None

    def detect_deadlock(self):
        """Scan the whole wait-for graph (used by the 'Check deadlock' button)."""
        with self._cond:
            for t in list(self.waits_for):
                cycle = self._find_cycle(t)
                if cycle:
                    return cycle
        return None

    def snapshot(self):
        with self._cond:
            return {"locks": dict(self.locks), "waits_for": dict(self.waits_for)}

    def clear(self):
        with self._cond:
            self.locks.clear()
            self.waits_for.clear()
            self._cond.notify_all()
