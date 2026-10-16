import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import scenarios
from engine import Engine
from lock_manager import BLOCKED, GRANTED, DeadlockError, LockManager
from thread_runner import run_threads


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.engine = Engine(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TestScenarios(Base):
    def test_all_eight_scenarios(self):
        for n in scenarios.SCENARIOS:
            r = scenarios.run(self.engine, n)
            self.assertTrue(r["passed"], f"Scenario {n} failed: expected {r['expected']} got {r['actual']}")

    def test_checkpoint_scans_fewer_records(self):
        r = scenarios.run(self.engine, 8)
        a = r["recovery"]["analysis"]
        self.assertLess(a["records_scanned"], a["total_records"])
        self.assertIsNotNone(a["checkpoint_lsn"])


class TestBasics(Base):
    def test_commit_then_flush_reaches_disk(self):
        t = self.engine.begin()["txn_id"]
        self.engine.write(t, "K", "1")
        self.assertNotIn("K", self.engine.storage.dump())      # only in buffer
        self.engine.commit(t)
        self.engine.flush()
        self.assertEqual(self.engine.storage.dump()["K"], "1")

    def test_abort_rolls_back(self):
        t = self.engine.begin()["txn_id"]
        self.engine.write(t, "K", "1")
        self.engine.abort(t)
        self.assertNotIn("K", self.engine.visible_database())

    def test_system_down_after_crash_until_recovery(self):
        self.engine.crash()
        self.assertEqual(self.engine.begin()["status"], "ERROR")
        self.engine.recover()
        self.assertEqual(self.engine.begin()["status"], "OK")

    def test_restart_runs_recovery_automatically(self):
        t = self.engine.begin()["txn_id"]
        self.engine.write(t, "K", "dirty")
        self.engine.flush()                       # uncommitted data reaches disk
        self.assertEqual(self.engine.storage.dump()["K"], "dirty")
        restarted = Engine(self.dir)              # simulates restarting the program
        self.assertNotIn("K", restarted.visible_database())

    def test_transaction_ids_continue_after_crash(self):
        t1 = self.engine.begin()["txn_id"]
        self.engine.commit(t1)
        self.engine.crash()
        self.engine.recover()
        t2 = self.engine.begin()["txn_id"]
        self.assertNotEqual(t1, t2)

    def test_recovery_is_idempotent(self):
        scenarios.run(self.engine, 4)
        before = self.engine.visible_database()
        self.engine.crash()
        self.engine.recover()
        self.engine.crash()
        self.engine.recover()
        self.assertEqual(before, self.engine.visible_database())


class TestLocks(unittest.TestCase):
    def test_conflict_and_deadlock(self):
        lm = LockManager()
        self.assertEqual(lm.acquire("T1", "X", blocking=False), GRANTED)
        self.assertEqual(lm.acquire("T2", "Y", blocking=False), GRANTED)
        self.assertEqual(lm.acquire("T1", "Y", blocking=False), BLOCKED)
        with self.assertRaises(DeadlockError):
            lm.acquire("T2", "X", blocking=False)


class TestThreads(Base):
    def test_threads_disjoint_keys(self):
        progs = {f"T{i}": [["write", f"K{i}", str(i)], ["commit"]] for i in range(1, 6)}
        out = run_threads(self.engine, progs)
        self.assertTrue(all(v == "COMPLETED" for v in out["results"].values()))
        self.assertEqual(self.engine.visible_database(),
                         {f"K{i}": str(i) for i in range(1, 6)})

    def test_threads_same_key_serialised(self):
        progs = {f"T{i}": [["write", "K", str(i)], ["sleep", 0.02], ["commit"]] for i in range(1, 5)}
        out = run_threads(self.engine, progs)
        self.assertTrue(all(v == "COMPLETED" for v in out["results"].values()))
        self.assertIn(self.engine.visible_database()["K"], {"1", "2", "3", "4"})

    def test_threads_forced_deadlock_has_one_victim(self):
        progs = {
            "T1": [["write", "X", "1"], ["sleep", 0.3], ["write", "Y", "1"], ["commit"]],
            "T2": [["write", "Y", "2"], ["sleep", 0.3], ["write", "X", "2"], ["commit"]],
        }
        out = run_threads(self.engine, progs, think_time=(0, 0))
        vals = sorted(out["results"].values())
        self.assertEqual(vals, ["COMPLETED", "DEADLOCK"])
        db = self.engine.visible_database()
        self.assertTrue(db == {"X": "1", "Y": "1"} or db == {"X": "2", "Y": "2"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
