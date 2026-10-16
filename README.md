# ARIES-Based Database Recovery and Concurrency Simulator  (PBL Phase 2)

## Run
    pip install flask
    python app.py            -> open http://127.0.0.1:5000
    python tests/test_all.py -> runs 12 automated tests (all 8 crash scenarios included)

## Who owns what
| Member | Modules (files) | OS concepts | DBMS concepts |
|---|---|---|---|
| **Sujanya Sailwal** (Storage & Logging) | `storage.py`, `wal_manager.py`, `buffer_manager.py` | file I/O, fsync, buffering / page cache, mutex on file | WAL, LSN, page LSN, dirty pages, durability, WAL rule |
| **Abhay Singh** (Concurrency & Crashes) | `lock_manager.py`, `crash_simulator.py`, `scenarios.py` | locks, mutual exclusion, condition variables, deadlock detection (wait-for graph), crash = memory loss | isolation, strict 2PL, failure points, 8 test scenarios |
| **Anuj Rawat** (Transactions & Recovery) | `transaction_manager.py`, `recovery.py`, `scheduler.py`, `engine.py` | CPU scheduling (FCFS / Round Robin, quantum), ready/blocked queues | transactions, ACID, commit/rollback, checkpoint, ARIES Analysis/Redo/Undo, CLR |
| **Palak Panai** (Threads, API & Dashboard) | `thread_runner.py`, `app.py`, `templates/index.html` | real threads, race conditions, thread-safe logging, request threads | REST interface, visualising log / buffer / locks / recovery |

## How the modules connect
    Dashboard (index.html) -> app.py (REST) -> engine.py
    engine.py -> transaction_manager -> lock_manager
                                     -> buffer_manager -> storage (database.db)
                                     -> wal_manager (wal.log)
    engine.py -> crash_simulator, recovery (ARIES)
    scheduler.py / thread_runner.py / scenarios.py  drive the engine

## Key design decisions (be ready to explain)
1. **No-force**: COMMIT only forces the log, not the data pages. After a crash, committed data may exist only in the log, so REDO is genuinely required (scenario 4).
2. **Steal**: a page of an uncommitted transaction may reach disk (scenario 3). So UNDO is genuinely required.
3. **WAL rule**: `buffer_manager.flush_page` asserts the log is already on disk up to the page's LSN.
4. **Repeat history**: REDO re-applies everything not yet on the page, even for transactions that will be undone later, using `page_lsn` to skip what is already on disk.
5. **CLRs**: every undo writes a Compensation Log Record, so a crash during recovery cannot undo the same change twice (tested: recovery is idempotent).
6. **Deadlock victim** = the transaction whose request closes the cycle; it is rolled back through the log and its locks are released.
7. **Restart = crash**: if the program starts and a log already exists, recovery runs automatically.

## Demo script (about 10 minutes)
1. Click scenario 4 -> "committed but only in the log" -> REDO restores it.
2. Click scenario 3 -> uncommitted data on disk -> UNDO removes it.
3. Click scenario 8 -> checkpoint -> Analysis scans far fewer records.
4. Scheduler box: default programs + Round Robin -> deadlock detected, victim rolled back.
5. Same programs with "Run with real threads" and `["sleep",0.3]` after the first write -> real OS threads deadlock.
6. Manual: Begin, Write, arm a crash point, Write -> system goes DOWN -> Run ARIES recovery.

## Limitations / future scope
* Single machine: if both the WAL file and the database file are destroyed, nothing can be recovered (needs replication / backups, which is outside ARIES).
* Only exclusive locks (no shared read locks); one key = one page; no indexes or SQL parsing.
