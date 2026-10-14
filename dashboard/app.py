"""Real-time leaderboard dashboard. Serves the latest top-10 computed by the
MapReduce job, read straight from HDFS (/tweets/trending/latest.json), and the
on/off switch for the tweet producer."""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import hdfs_client

from flask import Flask, jsonify, send_from_directory

HERE = os.path.dirname(os.path.abspath(__file__))
PRODUCER = os.path.join(config.ROOT, "producer", "tweet_producer.py")
HEARTBEAT_TIMEOUT_S = 10  # the producer reports about once a second
app = Flask(__name__)


@app.route("/")
def index():
    return send_from_directory(HERE, "index.html")


@app.route("/api/trending")
def trending():
    try:
        data = json.loads(hdfs_client.read(config.TRENDING_DIR + "/latest.json"))
    except Exception as e:
        return jsonify({"error": "No leaderboard on HDFS yet (%s)" % e}), 503
    data["window_minutes"] = config.WINDOW_MINUTES
    data["slide_seconds"] = config.SLIDE_SECONDS
    return jsonify(data)


def producer_state():
    try:
        with open(config.PRODUCER_STATE) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {"status": "stopped"}
    # a starting producer may spend ~20 s skipping ahead before its next heartbeat
    timeout_s = 60 if state.get("status") == "starting" else HEARTBEAT_TIMEOUT_S
    fresh = time.time() * 1000 - state.get("updated_ms", 0) < timeout_s * 1000
    if state.get("status") != "stopped" and not fresh:
        state["status"] = "stopped"  # window closed or process killed without saying so
    state["running"] = state["status"] in ("starting", "streaming", "paused")
    state["pause_requested"] = os.path.exists(config.PRODUCER_PAUSE)
    return state


@app.route("/api/system")
def system():
    """Component health written by scripts/supervisor.py."""
    try:
        with open(os.path.join(config.STATE_DIR, "system.json")) as f:
            state = json.load(f)
    except (OSError, ValueError):
        return jsonify({"supervised": False})
    state["supervised"] = time.time() * 1000 - state.get("updated_ms", 0) < 60000
    return jsonify(state)


@app.route("/api/producer")
def get_producer():
    return jsonify(producer_state())


@app.route("/api/producer/<action>", methods=["POST"])
def set_producer(action):
    os.makedirs(config.STATE_DIR, exist_ok=True)
    state = producer_state()
    if action == "off":
        open(config.PRODUCER_PAUSE, "w").close()
    elif action == "on":
        if os.path.exists(config.PRODUCER_PAUSE):
            os.remove(config.PRODUCER_PAUSE)
        if not state["running"]:
            # no producer to resume: start one where the last one stopped, in its own window
            command = '"%s" -u "%s" --loop --rate %d --skip %d' % (
                sys.executable, PRODUCER, state.get("rate") or config.PRODUCER_RATE, state.get("position") or 0)
            if os.name == "nt":  # titled console window, minimised and without taking focus
                info = subprocess.STARTUPINFO()
                info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = 7  # SW_SHOWMINNOACTIVE
                subprocess.Popen('cmd /c "title Tweet producer && %s"' % command, cwd=config.ROOT,
                                 creationflags=subprocess.CREATE_NEW_CONSOLE, startupinfo=info)
            else:
                subprocess.Popen(command, shell=True, cwd=config.ROOT)
            with open(config.PRODUCER_STATE, "w") as f:
                json.dump(dict(state, status="starting", updated_ms=int(time.time() * 1000)), f)
    else:
        return jsonify({"error": "action must be 'on' or 'off'"}), 400
    return jsonify(producer_state())


if __name__ == "__main__":
    print("Dashboard: http://localhost:%d" % config.DASHBOARD_PORT, flush=True)
    app.run(host="127.0.0.1", port=config.DASHBOARD_PORT, debug=False)
