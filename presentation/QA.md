# Likely questions and short answers

## Design

**Why Kafka, instead of reading the CSV straight into HDFS?**
Kafka decouples the tweet source from storage. Producers and consumers run at their own pace,
Kafka buffers bursts and keeps messages on disk, and consumers can resume from their committed offset after a crash.
In a real system the producer would be the Twitter API rather than a CSV replay.

**Why store on HDFS before computing, rather than counting in memory?**
Storing the raw events durably means any window can be recomputed, other jobs can reuse the data,
and the batch layer survives restarts. It is also what the problem statement asks for.

**How does the sliding window work?**
- The sink writes each tweet into a folder for the minute it arrived (`/tweets/raw/YYYYMMDD/HHMM`).
- Every 60 seconds the job reads the six minute-folders that overlap the last 5 minutes, and the mapper keeps only tweets whose timestamp is inside the exact window.
- So the window is 5 minutes long and slides by 1 minute, and each tweet is counted in about five consecutive runs.

**What do the mapper, combiner and reducer do?**
- **Mapper:** parses each tweet, keeps it if it falls inside the window, and emits `(#hashtag, 1)` once per hashtag per tweet.
- **Combiner:** sums the counts on the map side, which shrinks the data sent to the reducer.
- **Reducer:** sums the counts per hashtag and keeps the top 10 using a min-heap.
- A single reducer is used so that the top 10 is global.

**Isn't a single reducer a bottleneck?**
Not here. After the combiner it receives about 1,600 distinct hashtags per window.
At larger scale you would use several reducers, each producing a local top 10, then a tiny second job, or the driver, to merge them.

**Why does the job run in local mode, not on YARN?**
- It is still a real MapReduce job (Hadoop Streaming), and it reads from and writes to HDFS.
- Local mode runs it in one JVM on this single machine and finishes in 5 to 12 seconds.
- On YARN, container start-up alone takes tens of seconds, which is a large share of a 60-second cycle on a laptop.
- The same job can be submitted to YARN with `--framework yarn`.

**Why not Spark Streaming or Flink?**
The brief specifies MapReduce. Spark Structured Streaming or Flink, with windowed aggregations, would be the modern choice:
lower latency and no re-reading of five minutes of data each minute. That is a natural next step.

## Correctness and reliability

**What happens if a component crashes?**
- **Delivery is at-least-once:** the sink commits Kafka offsets only after the HDFS write succeeds, so a crash means re-reading, not losing data.
- **No half-written files:** files are written under a temporary `_tmp` name and then renamed, so a MapReduce job never reads a partial file.
- **Failed writes are retried:** if HDFS is unavailable the sink keeps its buffer and tries again.

**Can tweets be counted twice?**
Yes, in one edge case. If the sink crashes after writing to HDFS but before committing the offset, the tweets in that last batch are re-read.
Exactly-once would need idempotent writes, for example deduplicating by tweet id.

**What about the small-files problem?**
The sink writes one file per minute-folder about every 10 seconds, so around 6 files a minute.
The job uses `CombineTextInputFormat` to read them as one input split. In production you would compact old minutes into larger files.

**How fresh is the leaderboard?**
- Tweets reach HDFS within about 10 seconds.
- The job runs every 60 seconds and takes 5 to 12 seconds.
- The dashboard polls every 5 seconds.
- So what you see is at most about 70 seconds behind the stream.

## Data

**Why replay old tweets with today's timestamps?**
The datasets are from 2009 and 2015, so there is no live feed. Replaying them in their original order and stamping each with the arrival time simulates a stream.
The window is therefore based on processing time (when a tweet arrived), not event time (when it was originally posted).

**Why add the airline dataset?**
- Only about 2% of Sentiment140 tweets contain a hashtag.
- About 15% of the airline tweets do (2,254 of 14,640), with tags such as `#fail` and `#jetblue`.
- It is mixed in at one airline tweet per 110 Sentiment140 tweets.

**How are hashtags normalised?**
- Lower-cased, so `#FollowFriday` and `#followfriday` count together.
- Counted once per tweet, even if repeated.
- Pure numbers like `#1` are ignored.

**What is the sentiment label used for?**
Nothing yet. The task is trend detection. A good extension is showing each trending hashtag's mood,
for example "`#fail`: 85% negative", which only needs the mapper to emit the label alongside the hashtag.

## Limitations and future work

- **"Trending" here means most frequent in the window.** Always-popular tags like `#followfriday` therefore dominate.
  A better trend score compares the current window to a baseline, for example a growth rate or z-score, so sudden spikes rank higher.
- **Single-node, local-mode MapReduce.** The next steps are YARN on a cluster, or a move to Spark Structured Streaming.
- **Processing time, not event time.** A real feed needs event-time windows with allowance for late-arriving tweets.
- **No spam or bot filtering.** Some accounts post the same message repeatedly (`tweetpet`, for example).
- **Polling dashboard.** It checks every 5 seconds; it could push updates over WebSockets instead.
