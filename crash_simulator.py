

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
