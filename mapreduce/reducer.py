"""Reducer: sum counts per hashtag and emit only the top N most frequent.

Usage: reducer.py [top_n]

Must run as a single reducer so the top-N is global. Output lines:
    <hashtag>\t<count>      (top_n lines, most frequent first)
    __tweets__\t<n>         tweets in the window
    __distinct__\t<n>       distinct hashtags in the window
    __mentions__\t<n>       total hashtag mentions in the window
"""
import heapq
import sys
from itertools import groupby

top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 10

out = sys.stdout.buffer
totals = {b"__tweets__": 0, b"__distinct__": 0, b"__mentions__": 0}
top = []  # min-heap of (count, reversed-order tag) holding the best top_n so far

pairs = (line.rstrip(b"\r\n").split(b"\t", 1) for line in sys.stdin.buffer)
for key, group in groupby(pairs, key=lambda kv: kv[0]):
    count = sum(int(v) for _, v in group)
    if key.startswith(b"__"):
        totals[key] = totals.get(key, 0) + count
        continue
    totals[b"__distinct__"] += 1
    totals[b"__mentions__"] += count
    # ties broken alphabetically: negate the bytes so the heap evicts the later tag first
    item = (count, bytes(255 - b for b in key), key)
    if len(top) < top_n:
        heapq.heappush(top, item)
    elif item > top[0]:
        heapq.heapreplace(top, item)

for count, _, key in sorted(top, reverse=True):
    out.write(b"%s\t%d\n" % (key, count))
for key, count in totals.items():
    out.write(b"%s\t%d\n" % (key, count))
