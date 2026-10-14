"""Trend scheduler: every minute, run a Hadoop Streaming MapReduce job over the
last 5 minutes of tweets on HDFS and publish the top 10 hashtags.

Each run writes its job output to  /tweets/trending/runs/<window_end_ms>/
and the leaderboard the dashboard reads to  /tweets/trending/latest.json
(the top 10 plus the last 30 runs' counts, for trend lines)
"""
import argparse
import glob
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import hdfs_client

MR_DIR = os.path.join(config.ROOT, "mapreduce")
RUNS_DIR = config.TRENDING_DIR + "/runs"
LATEST = config.TRENDING_DIR + "/latest.json"
KEEP_RUNS = 30
TRACKED_N = 50      # the reducer keeps this many; the leaderboard shows the top config.TOP_N
HISTORY_RUNS = 30   # recent runs kept in latest.json for trend lines


def window_inputs(start_ms, end_ms):
    """HDFS minute directories that overlap [start, end) and hold data."""
    dirs = []
    for minute in range(start_ms // 60000, end_ms // 60000 + 1):
        t = time.gmtime(minute * 60)
        path = "%s/%s/%s" % (config.RAW_DIR, time.strftime("%Y%m%d", t), time.strftime("%H%M", t))
        if any(f["pathSuffix"].startswith("part-") for f in hdfs_client.listdir(path)):
            dirs.append(path)
    return dirs


def script(name):
    # absolute paths with forward slashes: no -files/symlinks needed on a single node
    return ("%s %s" % (sys.executable, os.path.join(MR_DIR, name))).replace("\\", "/")


def run_mapreduce(inputs, output, start_ms, end_ms, framework):
    streaming_jar = glob.glob(os.path.join(config.HADOOP_HOME, "share", "hadoop", "tools", "lib", "hadoop-streaming-*.jar"))[0]
    cmd = [
        os.path.join(config.HADOOP_HOME, "bin", "hadoop.cmd" if os.name == "nt" else "hadoop"),
        "jar", streaming_jar,
        "-D", "mapreduce.framework.name=" + framework,
        "-D", "mapreduce.job.name=trending-hashtags-%d" % end_ms,
        "-D", "mapreduce.job.reduces=1",
        # the sink writes many small files per minute; read them as one split
        "-D", "mapreduce.input.fileinputformat.split.maxsize=134217728",
        # with a non-default input format, streaming would prefix each line with its byte offset
        "-D", "stream.map.input.ignoreKey=true",
        "-inputformat", "org.apache.hadoop.mapred.lib.CombineTextInputFormat",
    ]
    for path in inputs:
        cmd += ["-input", path]
    cmd += [
        "-output", output,
        # parameters go on the command line: hadoop.cmd mangles "-cmdenv NAME=value" on Windows
        "-mapper", "%s %d %d" % (script("mapper.py"), start_ms, end_ms),
        "-combiner", script("combiner.py"),
        "-reducer", "%s %d" % (script("reducer.py"), TRACKED_N),
    ]
    env = dict(os.environ, HADOOP_HOME=config.HADOOP_HOME, JAVA_HOME=config.JAVA_HOME)
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, errors="replace")
    if result.returncode != 0:
        raise RuntimeError("MapReduce job failed:\n" + result.stderr[-3000:])


def read_output(output):
    top, totals = [], {}
    for f in hdfs_client.listdir(output):
        if not f["pathSuffix"].startswith("part-"):
            continue
        for line in hdfs_client.read("%s/%s" % (output, f["pathSuffix"])).decode("utf-8").splitlines():
            key, _, count = line.strip().partition("\t")
            if key.startswith("__"):
                totals[key.strip("_")] = int(count)
            elif key:
                top.append({"tag": key, "count": int(count)})
    top.sort(key=lambda r: (-r["count"], r["tag"]))
    return top, totals


def previous_result():
    try:
        return json.loads(hdfs_client.read(LATEST))
    except Exception:
        return {}


def prune_runs():
    runs = sorted(f["pathSuffix"] for f in hdfs_client.listdir(RUNS_DIR))
    for name in runs[:-KEEP_RUNS]:
        hdfs_client.delete("%s/%s" % (RUNS_DIR, name), recursive=True)


def run_once(framework):
    started = time.time()
    end_ms = int(started * 1000)
    start_ms = end_ms - config.WINDOW_MINUTES * 60000
    inputs = window_inputs(start_ms, end_ms)

    tracked, totals = [], {}
    if inputs:
        output = "%s/%d" % (RUNS_DIR, end_ms)
        run_mapreduce(inputs, output, start_ms, end_ms, framework)
        tracked, totals = read_output(output)

    previous = previous_result()
    before = {r["tag"]: r["rank"] for r in previous.get("top", [])}
    top = tracked[:config.TOP_N]
    for rank, row in enumerate(top, 1):
        row["rank"] = rank
        row["prev_rank"] = before.get(row["tag"])
    result = {
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "generated_at_ms": int(time.time() * 1000),
        "job_seconds": round(time.time() - started, 1),
        "tweets": totals.get("tweets", 0),
        "distinct_hashtags": totals.get("distinct", 0),
        "hashtag_mentions": totals.get("mentions", 0),
        "top": top,
    }
    # Recent runs for the dashboard's trend lines. Counts cover the top TRACKED_N tags,
    # so a hashtag's line stays continuous while it moves in and out of the top 10.
    snapshot = {k: result[k] for k in ("window_end_ms", "tweets", "distinct_hashtags", "hashtag_mentions", "job_seconds")}
    snapshot["counts"] = {r["tag"]: r["count"] for r in tracked}
    result["history"] = (previous.get("history", []) + [snapshot])[-HISTORY_RUNS:]
    hdfs_client.create(LATEST, json.dumps(result).encode("utf-8"), overwrite=True)
    prune_runs()

    print("%s  %d tweets in window, job took %.1fs" % (time.strftime("%H:%M:%S"), result["tweets"], result["job_seconds"]))
    for row in top:
        print("   %2d. %-28s %d" % (row["rank"], row["tag"], row["count"]))
    sys.stdout.flush()


def keep_awake():
    """Stop Windows from idle-sleeping while this process runs (sleep stalls Kafka and
    the whole stream). Nothing is changed permanently; closing the lid still sleeps."""
    if os.name == "nt":
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", help="run a single job and exit")
    ap.add_argument("--framework", choices=["local", "yarn"], default="local",
                    help="where the MapReduce job runs: 'local' (default, in-process runner reading HDFS, "
                         "starts in seconds) or 'yarn' (needs start-yarn.cmd)")
    args = ap.parse_args()

    hdfs_client.mkdirs(RUNS_DIR)
    keep_awake()
    print("Computing top %d hashtags over a sliding %d-minute window every %ds (Ctrl+C to stop)" %
          (config.TOP_N, config.WINDOW_MINUTES, config.SLIDE_SECONDS), flush=True)
    while True:
        tick = time.time()
        try:
            run_once(args.framework)
        except Exception as e:
            print("%s  run failed: %s" % (time.strftime("%H:%M:%S"), e), flush=True)
        if args.once:
            break
        try:
            time.sleep(max(0, config.SLIDE_SECONDS - (time.time() - tick)))
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
