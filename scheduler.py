"""
MODULE 8 - TRANSACTION SCHEDULER                  (Owner: Anuj Rawat)
==========================================================================
Decides WHICH transaction gets to run its next operation - exactly like an
OS CPU scheduler picks the next process.

  policy "fcfs"        : First-Come-First-Served, a transaction runs until it
                         finishes or gets blocked on a lock
  policy "round_robin" : each transaction gets `quantum` operations, then goes
                         to the back of the queue

A transaction that cannot get a lock is BLOCKED and put at the back of the
queue; it retries later. The result is a step-by-step timeline of the
interleaving, which makes lock conflicts and deadlocks easy to demonstrate
(and always reproducible).

OS concept   : CPU scheduling (FCFS, Round Robin, time quantum), ready/blocked
               queues, context switch.
"""
from collections import deque


class Scheduler:
    def __init__(self, engine, policy="round_robin", quantum=1):
        self.engine = engine
        self.policy = policy
        self.quantum = 10 ** 9 if policy == "fcfs" else max(1, int(quantum))

    def run(self, programs, crash_after=None, max_turns=500):
        """
        programs = {"T1": [["write","A","5"], ["commit"]], "T2": [...]}
        crash_after = crash the system after this many executed operations
        """
        eng = self.engine
        ids, pc, trace, queue = {}, {}, [], deque()
        for label in programs:
            ids[label] = eng.begin()["txn_id"]
            pc[label] = 0
            queue.append(label)
            trace.append({"step": 0, "txn": label, "op": "begin",
                          "result": f"started as {ids[label]}"})
        steps = turns = idle = 0
        outcome = "COMPLETED"

        while queue and turns < max_turns:
            turns += 1
            label = queue.popleft()
            ran, blocked, done = 0, False, False
            while ran < self.quantum and pc[label] < len(programs[label]):
                op = programs[label][pc[label]]
                if op[0] == "sleep":                    # not needed in simulation
                    pc[label] += 1
                    continue
                res = eng.execute(ids[label], op, blocking=False)
                status = res["status"]
                text = " ".join(str(x) for x in op)
                if status == "BLOCKED":
                    trace.append({"step": steps, "txn": label, "op": text,
                                  "result": f"BLOCKED - waiting for {res['waiting_for']}"})
                    blocked = True
                    break
                pc[label] += 1
                ran += 1
                steps += 1
                detail = status
                if status == "DEADLOCK":
                    detail = "DEADLOCK " + " -> ".join(res["cycle"]) + \
                             " : this transaction is the victim, rolled back"
                trace.append({"step": steps, "txn": label, "op": text, "result": detail})
                idle = 0
                if status in ("DEADLOCK", "ERROR", "TIMEOUT"):
                    done = True
                    break
                if status == "CRASHED":
                    return {"trace": trace, "outcome": "CRASHED", "txn_ids": ids}
                if crash_after is not None and steps >= crash_after:
                    eng.crash()
                    trace.append({"step": steps, "txn": "SYSTEM", "op": "CRASH",
                                  "result": f"system crashed after {steps} operations"})
                    return {"trace": trace, "outcome": "CRASHED", "txn_ids": ids}
            if pc[label] >= len(programs[label]):
                done = True
            if blocked:
                idle += 1
            if idle > len(queue) + 1:
                outcome = "STALLED"
                break
            if not done:
                queue.append(label)
        return {"trace": trace, "outcome": outcome, "txn_ids": ids}
