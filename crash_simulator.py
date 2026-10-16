"""
MODULE 5 - CRASH SIMULATOR                        (Owner: Abhay Singh)
==========================================================================
Lets us "pull the plug" at an exact moment inside a transaction.

 1. arm(point)   - choose where the crash will happen
 2. the transaction code calls  crash.check("POINT")  at those places
 3. when the armed point is reached, SimulatedCrash is raised and the
    engine wipes everything that lives only in memory (buffer, lock table,
    transaction table). The WAL file and database file stay on disk -
    exactly what a real power failure / process kill does.

OS concept   : process crash = loss of volatile memory, files survive;
               exceptions used to abort the running "process".
DBMS concept : failure points of a transaction.
"""

CRASH_POINTS = {
    "BEFORE_WAL":
        "Crash BEFORE the WAL record is written",
    "AFTER_WAL_BEFORE_UPDATE":
        "Crash AFTER the WAL record is written but BEFORE the page is updated",
    "AFTER_UPDATE_BEFORE_COMMIT":
        "Crash AFTER the updated page reached disk but BEFORE commit",
    "AFTER_COMMIT":
        "Crash AFTER the COMMIT record is logged, before data pages are flushed",
}


class SimulatedCrash(Exception):
    def __init__(self, point):
        self.point = point
        super().__init__(point)


class CrashSimulator:
    def __init__(self):
        self.armed = None

    def arm(self, point):
        if point not in CRASH_POINTS:
            raise ValueError(f"Unknown crash point: {point}")
        self.armed = point

    def disarm(self):
        self.armed = None

    def check(self, point, pre_crash=None):
        """Called by the transaction manager at every crash point."""
        if self.armed == point:
            self.armed = None
            if pre_crash:
                pre_crash()           # e.g. the OS had already written the page
            raise SimulatedCrash(point)
