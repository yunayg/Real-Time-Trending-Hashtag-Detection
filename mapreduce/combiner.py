"""Combiner: sum counts per key on the map side to shrink the shuffle."""
import sys
from itertools import groupby

out = sys.stdout.buffer
pairs = (line.rstrip(b"\r\n").split(b"\t", 1) for line in sys.stdin.buffer)
for key, group in groupby(pairs, key=lambda kv: kv[0]):
    out.write(b"%s\t%d\n" % (key, sum(int(v) for _, v in group)))
