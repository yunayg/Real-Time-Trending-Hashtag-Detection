"""Kafka -> HDFS sink.

Consumes tweet events and lands them on HDFS as JSON-lines files, bucketed by
the minute of the event timestamp:

    /tweets/raw/YYYYMMDD/HHMM/part-<millis>.jsonl      (UTC)

Offsets are committed only after a flush succeeds (at-least-once delivery).
"""
import json
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import hdfs_client

from kafka import KafkaConsumer
from kafka.errors import KafkaError

FLUSH_SECONDS = 10
FLUSH_RECORDS = 50000


def bucket_dir(ts_ms):
    t = time.gmtime(ts_ms / 1000)
    return "%s/%s/%s" % (config.RAW_DIR, time.strftime("%Y%m%d", t), time.strftime("%H%M", t))


def flush(buffers):
    """Write one file per minute bucket. Files are uploaded under a '_tmp' name
    (ignored by MapReduce) and renamed, so a job never reads a half-written file."""
    stamp = int(time.time() * 1000)
    for directory, lines in buffers.items():
        hdfs_client.mkdirs(directory)
        tmp = "%s/_tmp-%d" % (directory, stamp)
        hdfs_client.create(tmp, b"\n".join(lines) + b"\n", overwrite=True)
        hdfs_client.rename(tmp, "%s/part-%d.jsonl" % (directory, stamp))


def main():
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        group_id=config.KAFKA_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        max_poll_records=5000,
    )
    print("Sinking topic '%s' to hdfs:%s (Ctrl+C to stop)" % (config.KAFKA_TOPIC, config.RAW_DIR), flush=True)

    buffers, buffered, last_flush = defaultdict(list), 0, time.time()
    try:
        while True:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    try:
                        ts = json.loads(record.value)["ts"]
                    except (ValueError, KeyError, TypeError):
                        continue  # skip malformed events
                    buffers[bucket_dir(ts)].append(record.value)
                    buffered += 1
            if buffered and (time.time() - last_flush >= FLUSH_SECONDS or buffered >= FLUSH_RECORDS):
                try:
                    flush(buffers)
                except Exception as e:
                    # HDFS unavailable (e.g. still in safe mode): keep the buffer and retry,
                    # without committing offsets, so nothing is lost
                    print("%s  HDFS write failed, retrying in %ds: %s" % (time.strftime("%H:%M:%S"), FLUSH_SECONDS, e), flush=True)
                    last_flush = time.time()
                    continue
                try:
                    consumer.commit()
                except KafkaError as e:
                    # e.g. the group rebalanced after the machine slept; the data is already on
                    # HDFS, so carry on and let the consumer rejoin (some tweets may be re-read)
                    print("%s  offset commit failed, continuing: %s" % (time.strftime("%H:%M:%S"), e), flush=True)
                print("%s  wrote %d tweets to %d bucket(s): %s" % (
                    time.strftime("%H:%M:%S"), buffered, len(buffers), ", ".join(sorted(buffers))), flush=True)
                buffers, buffered, last_flush = defaultdict(list), 0, time.time()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
