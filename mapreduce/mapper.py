"""Mapper: tweet JSON lines -> "<hashtag>\t1" for tweets inside the time window.

Usage: mapper.py [window_start_ms window_end_ms]
A hashtag is counted once per tweet. Keys starting with "__" carry window totals.
"""
import json
import re
import sys

HASHTAG = re.compile(r"#(\w*[A-Za-z_]\w*)")  # at least one non-digit, so "#1" is not a hashtag

start = int(sys.argv[1]) if len(sys.argv) > 2 else 0
end = int(sys.argv[2]) if len(sys.argv) > 2 else 2 ** 62

# Binary I/O: keeps UTF-8 intact and avoids "\r\n" line endings on Windows.
out = sys.stdout.buffer
tweets = 0
for line in sys.stdin.buffer:
    try:
        event = json.loads(line)
        ts, text = event["ts"], event["text"]
    except (ValueError, KeyError, TypeError):
        continue
    if not start <= ts < end:
        continue
    tweets += 1
    for tag in set(t.lower() for t in HASHTAG.findall(text)):
        out.write(("#%s\t1\n" % tag).encode("utf-8"))

if tweets:
    out.write(b"__tweets__\t%d\n" % tweets)
