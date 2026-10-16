"""
MODULE 7 - ARIES RECOVERY                         (Owner: Anuj Rawat)
==========================================================================
Runs after a crash. Three passes over the WAL:

 1. ANALYSIS  Start at the last checkpoint and scan forward to rebuild
              - Transaction Table : who was running (losers) / committed (winners)
              - Dirty Page Table  : which pages might not be on disk
 2. REDO      "Repeat history": re-apply every UPDATE/CLR whose change is
              not yet on the page (page_lsn < record lsn) - even for
              transactions that will be undone later.
 3. UNDO      Roll back every loser transaction, newest change first,
              writing a CLR for each undo so that a crash DURING recovery
              never undoes the same change twice.

OS concept   : reading the log file sequentially, flushing buffers to disk.
DBMS concept : ARIES (Analysis, Redo, Undo), CLR, LSN, dirty page table.
"""


def recover(wal, buffer):
    log = wal.read_all()
    by_lsn = {r["lsn"]: r for r in log}

    # ------------------------------------------------------------- 1. ANALYSIS
    ck_pos = None
    for i, r in enumerate(log):
        if r["type"] == "CHECKPOINT":
            ck_pos = i
    txn_table, dpt, start = {}, {}, 0
    if ck_pos is not None:
        ck = log[ck_pos]
        txn_table = {t: {"status": "ACTIVE", "last_lsn": l}
                     for t, l in ck["txn_table"].items()}
        dpt = dict(ck["dpt"])
        start = ck_pos + 1

    def entry(t, lsn):
        return txn_table.setdefault(t, {"status": "ACTIVE", "last_lsn": lsn})

    scanned = 0
    for r in log[start:]:
        scanned += 1
        t, typ = r["txn_id"], r["type"]
        if typ == "BEGIN":
            txn_table[t] = {"status": "ACTIVE", "last_lsn": r["lsn"]}
        elif typ in ("UPDATE", "CLR"):
            entry(t, r["lsn"])["last_lsn"] = r["lsn"]
            dpt.setdefault(r["key"], r["lsn"])
        elif typ == "ABORT":
            entry(t, r["lsn"])["last_lsn"] = r["lsn"]
        elif typ == "COMMIT":
            e = entry(t, r["lsn"])
            e["status"], e["last_lsn"] = "COMMITTED", r["lsn"]
        elif typ == "END":
            txn_table.pop(t, None)

    winners = [t for t, e in txn_table.items() if e["status"] == "COMMITTED"]
    losers = [t for t, e in txn_table.items() if e["status"] == "ACTIVE"]
    analysis = {
        "checkpoint_lsn": log[ck_pos]["lsn"] if ck_pos is not None else None,
        "records_scanned": scanned,
        "total_records": len(log),
        "transaction_table": {t: dict(e) for t, e in txn_table.items()},
        "dirty_page_table": dict(dpt),
        "winners": winners,
        "losers": losers,
    }

    # ---------------------------------------------------------------- 2. REDO
    redo = []
    if dpt:
        redo_start = min(dpt.values())
        for r in log:
            if r["lsn"] < redo_start or r["type"] not in ("UPDATE", "CLR"):
                continue
            key = r["key"]
            ev = {"lsn": r["lsn"], "type": r["type"], "txn_id": r["txn_id"], "key": key}
            if key not in dpt:
                ev.update(action="SKIPPED", reason="page was not dirty at crash")
            elif r["lsn"] < dpt[key]:
                ev.update(action="SKIPPED", reason="older than the page's recLSN")
            elif buffer.page_lsn(key) >= r["lsn"]:
                ev.update(action="SKIPPED",
                          reason=f"page on disk already has LSN {buffer.page_lsn(key)}")
            else:
                buffer.put(key, r["new"], r["lsn"])
                ev.update(action="REDONE", reason=f"{key} = {r['new']}")
            redo.append(ev)

    # ---------------------------------------------------------------- 3. UNDO
    undo = []
    txn_last = {t: txn_table[t]["last_lsn"] for t in losers}
    todo = set(txn_last.values())
    while todo:
        lsn = max(todo)                           # always undo the newest first
        todo.discard(lsn)
        r = by_lsn[lsn]
        t = r["txn_id"]
        if r["type"] == "UPDATE":
            clr = wal.append("CLR", t, r["key"], old=r["new"], new=r["old"],
                             prev_lsn=txn_last[t], undo_next_lsn=r["prev_lsn"])
            txn_last[t] = clr
            buffer.put(r["key"], r["old"], clr)
            undo.append({"lsn": lsn, "txn_id": t, "key": r["key"], "action": "UNDONE",
                         "reason": f"{r['key']} restored to {r['old']} (CLR {clr})"})
            nxt = r["prev_lsn"]
        elif r["type"] == "CLR":
            nxt = r["undo_next_lsn"]
        else:
            nxt = r["prev_lsn"]
        if nxt is None:
            wal.append("END", t, prev_lsn=txn_last[t])
            undo.append({"lsn": lsn, "txn_id": t, "key": None, "action": "ENDED",
                         "reason": f"{t} fully rolled back"})
        else:
            todo.add(nxt)

    for t in winners:                             # committed work is finished
        wal.append("END", t, prev_lsn=txn_table[t]["last_lsn"])

    buffer.flush_all()
    wal.append("CHECKPOINT", txn_table={}, dpt={})
    return {"analysis": analysis, "redo": redo, "undo": undo}
