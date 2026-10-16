"""
MODULE 10 - THREAD RUNNER                         (Owner: Palak Panai)
==========================================================================
Runs every transaction in its OWN OS THREAD, so they really execute at the
same time. Threads that need a locked key go to sleep on a condition
variable and are woken when the lock is released. Random "think time"
between operations makes every run interleave differently.

Use  ["sleep", 0.2]  inside a program to hold locks long enough to
guarantee overlap (e.g. to force a deadlock).

OS concept   : threads, concurrency, race conditions, blocking/waking,
               thread-safe logging with a mutex.
"""
import random
import threading
import time


def run_threads(engine, programs, think_time=(0.0, 0.03), lock_timeout=3.0):
    trace, results = [], {}
    guard = threading.Lock()
    t0 = time.time()

    def log(label, event):
        with guard:
            trace.append({"time": round(time.time() - t0, 3), "thread": label, "event": event})

    def worker(label, ops):
        tid = engine.begin()["txn_id"]
        log(label, f"begin -> {tid}")
        for op in ops:
            time.sleep(random.uniform(*think_time))
            res = engine.execute(tid, op, blocking=True, timeout=lock_timeout)
            log(label, f"{' '.join(str(x) for x in op)} -> {res['status']}")
            if res["status"] not in ("OK", "COMMITTED", "ABORTED"):
                results[label] = res["status"]
                return
        results[label] = "COMPLETED"

    threads = [threading.Thread(target=worker, args=(label, ops), name=label)
               for label, ops in programs.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return {"trace": trace, "results": results}
