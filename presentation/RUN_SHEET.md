# Demo run sheet

## The night before

- [ ] Charge the laptop and bring the charger. Plan to stay plugged in.
- [ ] Do one full rehearsal using the steps below, then run `scripts\stop_all.ps1 -IncludeHdfs`.
- [ ] Bookmark http://localhost:5000/?theme=light (light mode reads better on projectors)
      and http://localhost:9870/explorer.html#/tweets (the HDFS file browser).
- [ ] Backup screenshots are in this folder (`dashboard-light.png`, `dashboard-dark.png`) in case the live demo fails.

## 15 minutes before you present

The laptop has 7.7 GB of RAM and the pipeline uses about 3 GB, so close Chrome tabs, Teams and other heavy apps first.

Open PowerShell in the project folder (`C:\Users\admin\OneDrive\Desktop\bda`):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_services.ps1
```
This takes about 1 minute. It starts HDFS, ZooKeeper and Kafka in their own windows and should end with "Services ready".

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_pipeline.ps1 -Skip 1220000
```
This opens 4 more windows (HDFS sink, trend job, dashboard, producer) and then the dashboard in your browser.

- **What `-Skip 1220000` does:** it starts the replay in mid-June 2009, where `#iranelection` (the Iranian election protests),
  `#iphone` (the iPhone 3GS launch) and `#squarespace` trend. It's a better story than April, where `#followfriday` dominates.
  The producer takes about 20 seconds to skip ahead before tweets start flowing.
- **Wait about 5 minutes before presenting**, so the 5-minute window is full and the trend lines have a few points.

**Windows you should see:** 2 for HDFS (namenode, datanode), ZooKeeper, Kafka, HDFS sink, Trend job, Dashboard and Tweet producer.
Don't close them. Minimise them, and arrange the producer, sink and trend job windows so you can show them.

## Demo flow (about 5 minutes)

1. **Problem.** Trends computed in periodic batch jobs are noticed only after they peak. This pipeline updates every minute.
2. **Producer window.** It replays Sentiment140 plus airline tweets into Kafka at about 500 tweets per second.
   Point at the "replaying Jun 15 2009" date: old tweets are re-stamped with the current time to simulate a live feed.
3. **HDFS sink window.** Every ~10 seconds it writes a file into a per-minute folder.
   Open http://localhost:9870/explorer.html#/tweets/raw and click into today's date, then a minute folder, to show the `part-*.jsonl` files.
4. **Trend job window.** Every 60 seconds a Hadoop Streaming MapReduce job runs over the last 5 minutes of folders,
   takes 5 to 12 seconds, and prints the top 10. Show the job output in HDFS at `/tweets/trending/runs/<latest>/part-00000`.
5. **Dashboard.** Walk through it:
   - the #1 spotlight card;
   - the stat tiles (tweets in the window, mentions, distinct hashtags, job time);
   - the leaderboard: arrows for rank movement, NEW for new entries, and a sparkline of the last 15 runs per hashtag;
   - the countdown ring. Let it reach zero during your talk: the rows animate into their new order when the next run lands.
6. **Code, if asked.** Open `mapreduce\mapper.py`, `combiner.py` and `reducer.py`. Each is about 20 lines.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Dashboard says "Waiting for data" or "Stale" | Look at the Trend job window for an error. Check the producer window is still printing "sent ... tweets". |
| Leaderboard says "No hashtags in the last 5 minutes" | The producer isn't running or is still skipping ahead. Wait 30 seconds, or restart it: `python producer\tweet_producer.py --skip 1220000` |
| Kafka window closes straight away | Run `start_services.ps1` again. It retries Kafka automatically. |
| Sink window says "HDFS write failed, retrying" | HDFS is still starting (safe mode). It recovers by itself, and nothing is lost. |
| Browser can't reach localhost:5000 | The Dashboard window was closed. Run `python dashboard\app.py` in the project folder. |
| Everything is broken | `scripts\stop_all.ps1 -IncludeHdfs`, then both start scripts again. That takes about 2 minutes. If there's no time, use the backup screenshots. |
| Laptop went to sleep | Wake it. The stages recover by themselves within a minute or two. The trend job stops idle sleep while it runs, but closing the lid still sleeps. |

## Afterwards

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop_all.ps1 -IncludeHdfs
```

## Numbers worth quoting

| | |
|---|---|
| Stream rate | ~500 tweets/s |
| Tweets in a 5-minute window | ~140,000 |
| Hashtag mentions per window | ~3,000 (only ~2% of Sentiment140 tweets have a hashtag) |
| Distinct hashtags per window | ~1,600 |
| Kafka to HDFS delay | 10 seconds or less |
| MapReduce job time | 5 to 12 seconds |
| Leaderboard freshness | at most ~70 seconds behind the stream |
| Cold start | ~1 minute for services, then the first leaderboard within a minute |
