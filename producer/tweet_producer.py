"""Replay Sentiment140 + Twitter US Airline tweets into Kafka as a live stream.

Tweets are replayed in their original chronological order, but each event is
stamped with the current time ("ts") so the pipeline sees them as happening now.
"""
import argparse
import csv
import heapq
import itertools
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

from kafka import KafkaProducer

MONTHS = {m: i for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}


def _s140_key(row):
    # "Mon Apr 06 22:19:45 PDT 2009" -> sortable tuple
    p = row[2].split()
    return (p[5], MONTHS[p[1]], p[2], p[3])


def _s140_class(path, target):
    with open(path, encoding="latin-1", newline="") as f:
        for row in csv.reader(f):
            if row[0] == target:
                yield row


def sentiment140(path):
    """The file holds all negative tweets then all positive ones, each block in
    time order, so merging the two blocks gives one chronological stream."""
    merged = heapq.merge(_s140_class(path, "0"), _s140_class(path, "4"), key=_s140_key)
    for _, tweet_id, date, _, user, text in merged:
        yield {"id": tweet_id, "created_at": date, "user": user, "text": text, "source": "sentiment140"}


def airline(path):
    with open(path, encoding="utf-8", newline="") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r["tweet_created"])
    for r in rows:
        yield {"id": r["tweet_id"], "created_at": r["tweet_created"], "user": r["name"],
               "text": r["text"], "source": "airline"}


def tweets(airline_every):
    """Sentiment140 stream with one airline tweet mixed in every `airline_every` tweets."""
    extra = airline(config.AIRLINE_CSV)
    for i, tweet in enumerate(sentiment140(config.SENTIMENT140_CSV), 1):
        yield tweet
        if i % airline_every == 0:
            supplement = next(extra, None)
            if supplement:
                yield supplement
    yield from extra


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rate", type=int, default=500, help="tweets per second (default 500)")
    ap.add_argument("--skip", type=int, default=0, help="skip this many tweets before starting")
    ap.add_argument("--limit", type=int, default=0, help="stop after this many tweets (0 = all)")
    ap.add_argument("--airline-every", type=int, default=110,
                    help="mix in one airline tweet per N Sentiment140 tweets (default 110)")
    ap.add_argument("--loop", action="store_true", help="start the replay again when it reaches the end")
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, linger_ms=50)

    position = [args.skip]  # tweets into the replay, so a restart can resume from here

    def replay():
        skip = args.skip  # only the first pass starts part-way in
        while True:
            for tweet in itertools.islice(tweets(args.airline_every), skip, None):
                position[0] += 1
                yield tweet
            if not args.loop:
                return
            skip, position[0] = 0, 0
            print("reached the end of the datasets, starting the replay again from April 2009", flush=True)

    status = {"status": "starting", "pid": os.getpid(), "rate": args.rate, "sent": 0, "replaying": None}

    def heartbeat(state):
        status.update(state, position=position[0], updated_ms=int(time.time() * 1000))
        try:
            os.makedirs(config.STATE_DIR, exist_ok=True)
            tmp = config.PRODUCER_STATE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(status, f)
            os.replace(tmp, config.PRODUCER_STATE)
        except OSError:
            pass  # status reporting must never stop the stream

    # a fresh start is never paused, even if the switch was left off
    if os.path.exists(config.PRODUCER_PAUSE):
        os.remove(config.PRODUCER_PAUSE)
    heartbeat({})

    stream = itertools.islice(replay(), args.limit or None)
    tick = 0.1
    per_tick = max(1, int(args.rate * tick))
    sent, started, last_report, last_beat, paused = 0, time.time(), time.time(), 0, False
    print("Producing to topic '%s' at ~%d tweets/s (Ctrl+C to stop)" % (config.KAFKA_TOPIC, args.rate), flush=True)
    try:
        while True:
            tick_started = time.time()
            if os.path.exists(config.PRODUCER_PAUSE):
                if not paused:
                    paused = True
                    print("paused from the dashboard", flush=True)
                if tick_started - last_beat >= 1:
                    last_beat = tick_started
                    heartbeat({"status": "paused"})
                time.sleep(0.5)
                continue
            if paused:
                paused = False
                print("resumed from the dashboard", flush=True)
            batch = list(itertools.islice(stream, per_tick))
            if not batch:
                break
            now_ms = int(tick_started * 1000)
            for tweet in batch:
                tweet["ts"] = now_ms
                producer.send(config.KAFKA_TOPIC, json.dumps(tweet).encode("utf-8"))
            sent += len(batch)
            if tick_started - last_beat >= 1:
                last_beat = tick_started
                heartbeat({"status": "streaming", "sent": sent, "replaying": batch[-1]["created_at"]})
            if tick_started - last_report >= 5:
                last_report = tick_started
                print("sent %d tweets (%.0f/s), replaying %s" %
                      (sent, sent / (tick_started - started), batch[-1]["created_at"]), flush=True)
            time.sleep(max(0, tick - (time.time() - tick_started)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        heartbeat({"status": "stopped", "sent": sent})
        print("done, sent %d tweets" % sent, flush=True)


if __name__ == "__main__":
    main()
