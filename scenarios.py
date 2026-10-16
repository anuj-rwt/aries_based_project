"""
MODULE 11 - CRASH SCENARIOS                       (Owner: Abhay Singh)
==========================================================================
The 8 test cases from our notebook, each one scripted end-to-end:
  setup -> run -> crash -> ARIES recovery -> compare with expected result.

Every scenario starts from the same committed baseline on disk:
        AccountA = 5000     AccountB = 3000
"""
from scheduler import Scheduler

BASE = {"AccountA": "5000", "AccountB": "3000"}


def _baseline(engine, say):
    engine.reset()
    t = engine.begin()["txn_id"]
    engine.write(t, "AccountA", "5000")
    engine.write(t, "AccountB", "3000")
    engine.commit(t)
    engine.flush()
    say("Baseline committed and flushed to disk: AccountA=5000, AccountB=3000")


def s1(engine, say):
    _baseline(engine, say)
    t = engine.begin()["txn_id"]
    engine.arm_crash("BEFORE_WAL")
    r = engine.write(t, "AccountA", "4000")
    say(f"{t} writes AccountA=4000 -> {r['status']} ({r['message']})")
    say("Nothing was logged for the update, so there is nothing to redo or undo")
    return dict(BASE), engine.recover()


def s2(engine, say):
    _baseline(engine, say)
    t = engine.begin()["txn_id"]
    engine.arm_crash("AFTER_WAL_BEFORE_UPDATE")
    r = engine.write(t, "AccountA", "4000")
    say(f"{t} writes AccountA=4000 -> {r['status']} ({r['message']})")
    say("The log has the UPDATE but the page never changed; recovery redoes it, then undoes it")
    return dict(BASE), engine.recover()


def s3(engine, say):
    _baseline(engine, say)
    t = engine.begin()["txn_id"]
    engine.arm_crash("AFTER_UPDATE_BEFORE_COMMIT")
    r = engine.write(t, "AccountA", "4000")
    say(f"{t} writes AccountA=4000 -> {r['status']} ({r['message']})")
    say("Uncommitted data (4000) IS on disk; REDO skips it, UNDO must remove it")
    return dict(BASE), engine.recover()


def s4(engine, say):
    _baseline(engine, say)
    t = engine.begin()["txn_id"]
    engine.write(t, "AccountA", "4000")
    engine.arm_crash("AFTER_COMMIT")
    r = engine.commit(t)
    say(f"{t} commits AccountA=4000 -> {r['status']} ({r['message']})")
    say("COMMIT is in the log but the page was never flushed; REDO must restore 4000")
    return {"AccountA": "4000", "AccountB": "3000"}, engine.recover()


def s5(engine, say):
    _baseline(engine, say)
    programs = {
        "T1": [["write", "AccountA", "4000"], ["commit"]],
        "T2": [["write", "AccountB", "1000"], ["write", "Temp", "7"]],
        "T3": [["write", "Audit", "x"]],
    }
    out = Scheduler(engine, "round_robin", 1).run(programs, crash_after=5)
    for e in out["trace"]:
        say(f"step {e['step']}: {e['txn']} {e['op']} -> {e['result']}")
    say("T1 committed; T2 and T3 were still running when the system crashed")
    return {"AccountA": "4000", "AccountB": "3000"}, engine.recover()


def s6(engine, say):
    _baseline(engine, say)
    programs = {
        "T1": [["write", "AccountA", "4000"], ["commit"]],
        "T2": [["write", "AccountA", "4500"], ["commit"]],
    }
    out = Scheduler(engine, "round_robin", 1).run(programs)
    for e in out["trace"]:
        say(f"step {e['step']}: {e['txn']} {e['op']} -> {e['result']}")
    say("T2 had to wait for T1's lock, then continued. Now crash to prove durability")
    engine.crash()
    return {"AccountA": "4500", "AccountB": "3000"}, engine.recover()


def s7(engine, say):
    _baseline(engine, say)
    programs = {
        "T1": [["write", "X", "1"], ["write", "Y", "1"], ["commit"]],
        "T2": [["write", "Y", "2"], ["write", "X", "2"], ["commit"]],
    }
    out = Scheduler(engine, "round_robin", 1).run(programs)
    for e in out["trace"]:
        say(f"step {e['step']}: {e['txn']} {e['op']} -> {e['result']}")
    say("Wait-for graph had a cycle; the victim was rolled back, the other finished")
    engine.crash()
    return {"AccountA": "5000", "AccountB": "3000", "X": "1", "Y": "1"}, engine.recover()


def s8(engine, say):
    _baseline(engine, say)
    t1 = engine.begin()["txn_id"]
    engine.write(t1, "AccountA", "4000")
    engine.commit(t1)
    ck = engine.checkpoint()
    say(f"{t1} committed AccountA=4000, checkpoint at LSN {ck['lsn']} flushed {ck['flushed_pages']}")
    t2 = engine.begin()["txn_id"]
    engine.write(t2, "AccountB", "2500")
    say(f"{t2} wrote AccountB=2500 but did not commit; now the system crashes")
    engine.crash()
    rec = engine.recover()
    a = rec["analysis"]
    say(f"Analysis scanned only {a['records_scanned']} of {a['total_records']} log records")
    return {"AccountA": "4000", "AccountB": "3000"}, rec


SCENARIOS = {
    1: ("Crash before WAL record is written",
        "Nothing reached the log, so recovery has nothing to fix.", s1),
    2: ("Crash after WAL, before the page update",
        "Log has the change, the page does not: Redo re-applies it, Undo then removes it.", s2),
    3: ("Crash after DB update, before commit",
        "Uncommitted data is on disk: Undo must roll it back using the old value from the log.", s3),
    4: ("Crash after commit, before pages are flushed",
        "Committed but only in the log: Redo restores it. Durability (ACID 'D').", s4),
    5: ("Concurrent transactions crash",
        "Winners (committed) are redone, losers (still running) are undone.", s5),
    6: ("Lock conflict simulation",
        "Second transaction waits for the first one's lock; committed results survive a crash.", s6),
    7: ("Deadlock simulation",
        "Wait-for graph finds the cycle, victim is rolled back, the other transaction completes.", s7),
    8: ("Checkpoint simulation",
        "Analysis starts at the checkpoint, so far fewer log records have to be scanned.", s8),
}


def listing():
    return [{"id": n, "title": t, "explain": e} for n, (t, e, _) in SCENARIOS.items()]


def run(engine, n):
    title, explain, fn = SCENARIOS[n]
    steps = []
    expected, rec = fn(engine, steps.append)
    actual = engine.visible_database()
    return {"id": n, "title": title, "explain": explain, "steps": steps,
            "expected": expected, "actual": actual,
            "passed": expected == actual, "recovery": rec}
