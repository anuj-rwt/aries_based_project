"""
MODULE 12 - REST API                              (Owner: Palak Panai)
==========================================================================
Flask web server. It only translates HTTP requests into Engine calls and
returns JSON; all the real work happens in the other modules.

Run:   pip install flask
       python app.py          then open http://127.0.0.1:5000

OS concept   : the web server runs request-handler threads in parallel
               (threaded=True), all sharing one Engine safely through locks.
"""
from flask import Flask, jsonify, render_template, request

import scenarios
from crash_simulator import CRASH_POINTS
from engine import Engine
from scheduler import Scheduler
from thread_runner import run_threads

app = Flask(__name__)
engine = Engine("data")


def body():
    return request.get_json(silent=True) or {}


@app.route("/")
def index():
    return render_template("index.html", crash_points=CRASH_POINTS)


@app.route("/api/state")
def api_state():
    return jsonify(engine.state())


@app.route("/api/wal")
def api_wal():
    return jsonify(engine.wal_records())


@app.route("/api/begin", methods=["POST"])
def api_begin():
    return jsonify(engine.begin())


@app.route("/api/write", methods=["POST"])
def api_write():
    b = body()
    return jsonify(engine.write(b["txn_id"], b["key"], b["value"], blocking=False))


@app.route("/api/read", methods=["POST"])
def api_read():
    b = body()
    return jsonify(engine.read(b["txn_id"], b["key"], blocking=False))


@app.route("/api/commit", methods=["POST"])
def api_commit():
    return jsonify(engine.commit(body()["txn_id"]))


@app.route("/api/abort", methods=["POST"])
def api_abort():
    return jsonify(engine.abort(body()["txn_id"]))


@app.route("/api/flush", methods=["POST"])
def api_flush():
    return jsonify(engine.flush())


@app.route("/api/checkpoint", methods=["POST"])
def api_checkpoint():
    return jsonify(engine.checkpoint())


@app.route("/api/arm-crash", methods=["POST"])
def api_arm():
    engine.arm_crash(body()["point"])
    return jsonify({"status": "ARMED", "point": body()["point"]})


@app.route("/api/crash", methods=["POST"])
def api_crash():
    engine.crash()
    return jsonify({"status": "CRASHED"})


@app.route("/api/recover", methods=["POST"])
def api_recover():
    return jsonify(engine.recover())


@app.route("/api/deadlock-check")
def api_deadlock():
    cycle = engine.locks.detect_deadlock()
    return jsonify({"deadlock": bool(cycle), "cycle": cycle})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    engine.reset()
    return jsonify({"status": "RESET"})


@app.route("/api/run-scheduler", methods=["POST"])
def api_scheduler():
    b = body()
    s = Scheduler(engine, b.get("policy", "round_robin"), b.get("quantum", 1))
    return jsonify(s.run(b["programs"], crash_after=b.get("crash_after")))


@app.route("/api/run-threads", methods=["POST"])
def api_threads():
    return jsonify(run_threads(engine, body()["programs"]))


@app.route("/api/scenarios")
def api_scenarios():
    return jsonify(scenarios.listing())


@app.route("/api/scenario/<int:n>", methods=["POST"])
def api_scenario(n):
    return jsonify(scenarios.run(engine, n))


if __name__ == "__main__":
    app.run(debug=False, threaded=True)
