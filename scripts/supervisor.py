"""Supervisor: keeps the whole pipeline running, including after the laptop sleeps.

Every few seconds it checks HDFS, ZooKeeper, Kafka and the four pipeline stages,
and starts anything that is not running (each in its own window), in dependency
order. It writes a status summary for the dashboard to state/system.json.

    python scripts/supervisor.py

Stop everything with scripts/stop_all.ps1 (which stops the supervisor first).
To keep the producer off, use the dashboard switch: a paused producer is left alone.
"""
import json
import os
import socket
import subprocess
import sys
import time

import psutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

CHECK_SECONDS = 10
KAFKA_HOME = r"C:\Kafka\kafka_2.12-2.3.1\kafka_2.12-2.3.1"
SYSTEM_STATE = os.path.join(config.STATE_DIR, "system.json")
PY = sys.executable


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def process_running(pattern):
    for p in psutil.process_iter(["name", "cmdline"]):
        try:
            if p.info["name"] and p.info["name"].lower() in ("python.exe", "java.exe", "python", "java"):
                if pattern in " ".join(p.info["cmdline"] or []):
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def env():
    e = dict(os.environ, HADOOP_HOME=config.HADOOP_HOME, JAVA_HOME=config.JAVA_HOME,
             KAFKA_HEAP_OPTS="-Xmx512M -Xms512M")  # avoids the start script's call to wmic
    e["PATH"] = os.pathsep.join([os.path.join(config.HADOOP_HOME, "bin"), os.path.join(config.JAVA_HOME, "bin"), e["PATH"]])
    return e


def background_window():
    """Popen arguments for a new console window that starts minimised without taking focus."""
    if os.name != "nt":
        return {}
    info = subprocess.STARTUPINFO()
    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    info.wShowWindow = 7  # SW_SHOWMINNOACTIVE
    return {"creationflags": subprocess.CREATE_NEW_CONSOLE, "startupinfo": info}


def launch(title, command, cwd=config.ROOT):
    """Start a command in its own minimised console window.
    Passed as one string: a list would make Python escape the inner quotes as \\",
    which cmd does not understand. /c (not /k) closes the window if the program exits,
    so a crash does not leave a dead window behind; the supervisor restarts it."""
    subprocess.Popen('cmd /c "title %s && %s"' % (title, command), cwd=cwd, env=env(), **background_window())


def producer_command():
    """Resume the replay where the last producer stopped."""
    try:
        with open(config.PRODUCER_STATE) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    return '"%s" -u producer\\tweet_producer.py --loop --rate %d --skip %d' % (
        PY, state.get("rate") or config.PRODUCER_RATE, state.get("position") or 0)


hdfs = os.path.join(config.HADOOP_HOME, "bin", "hdfs.cmd")
# name, is-it-running check, how to start it, what must be up first
COMPONENTS = [
    ("NameNode", lambda: port_open(9000), lambda: launch("Apache Hadoop Distribution - hadoop   namenode", '"%s" namenode' % hdfs), []),
    ("DataNode", lambda: process_running("datanode.DataNode"), lambda: launch("Apache Hadoop Distribution - hadoop   datanode", '"%s" datanode' % hdfs), ["NameNode"]),
    ("ZooKeeper", lambda: port_open(2181), lambda: launch("ZooKeeper", r"bin\windows\zookeeper-server-start.bat config\zookeeper.properties", KAFKA_HOME), []),
    ("Kafka", lambda: port_open(9092), lambda: launch("Kafka", r"bin\windows\kafka-server-start.bat config\server.properties", KAFKA_HOME), ["ZooKeeper"]),
    ("HDFS sink", lambda: process_running("kafka_to_hdfs.py"), lambda: launch("HDFS sink (Kafka to HDFS)", '"%s" -u consumer\\kafka_to_hdfs.py' % PY), ["Kafka", "DataNode"]),
    ("Trend job", lambda: process_running("run_trending_job.py"), lambda: launch("Trend job (MapReduce)", '"%s" -u scripts\\run_trending_job.py' % PY), ["DataNode"]),
    ("Dashboard", lambda: process_running("dashboard\\app.py"), lambda: launch("Dashboard", '"%s" -u dashboard\\app.py' % PY), ["NameNode"]),
    ("Producer", lambda: process_running("tweet_producer.py"), lambda: launch("Tweet producer", producer_command()), ["Kafka"]),
]
# services that need a while to come up are not started twice in quick succession
GRACE_SECONDS = {"NameNode": 60, "DataNode": 45, "ZooKeeper": 30, "Kafka": 40}


def write_state(status, woke_at):
    os.makedirs(config.STATE_DIR, exist_ok=True)
    tmp = SYSTEM_STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"updated_ms": int(time.time() * 1000), "woke_at_ms": woke_at, "components": status}, f)
    os.replace(tmp, SYSTEM_STATE)


def main():
    print("Supervisor: checking the pipeline every %ds (Ctrl+C to stop)" % CHECK_SECONDS, flush=True)
    last_start, last_loop, woke_at = {}, time.time(), None
    attempts = {}  # recent start attempts per component, to back off if one keeps failing
    while True:
        now = time.time()
        if now - last_loop > 6 * CHECK_SECONDS:
            woke_at = int(now * 1000)
            print("%s  woke up after %.0f min, checking everything" % (time.strftime("%H:%M:%S"), (now - last_loop) / 60), flush=True)
        last_loop = now

        status = {}
        for name, is_up, start, needs in COMPONENTS:
            if is_up():
                status[name] = "running"
                attempts.pop(name, None)
                continue
            if name == "Producer" and os.path.exists(config.PRODUCER_PAUSE):
                status[name] = "off"  # switched off on the dashboard: leave it off
                continue
            if any(status.get(n) != "running" for n in needs):
                status[name] = "waiting"
                continue
            # back off after repeated failures: 3 quick tries, then one try every 5 minutes
            recent = [t for t in attempts.get(name, []) if now - t < 300]
            wait = GRACE_SECONDS.get(name, 20) if len(recent) < 3 else 300
            if now - last_start.get(name, 0) < wait:
                status[name] = "starting" if len(recent) < 3 else "failing"
                continue
            print("%s  %s is not running, starting it" % (time.strftime("%H:%M:%S"), name), flush=True)
            start()
            last_start[name] = now
            attempts[name] = recent + [now]
            status[name] = "starting"
        write_state(status, woke_at)
        try:
            time.sleep(CHECK_SECONDS)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
