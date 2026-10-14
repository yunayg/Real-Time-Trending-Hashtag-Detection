# Demo run sheet

## The night before

- [ ] Charge the laptop and bring the charger. Plan to stay plugged in.
- [ ] Do one full rehearsal using the steps below, then run `scripts\stop_all.ps1 -IncludeHdfs`.
- [ ] Bookmark http://localhost:5000/?theme=light (light mode reads better on projectors)
      and http://localhost:9870/explorer.html#/tweets (the HDFS file browser).
- [ ] Backup screenshots are in this folder (`dashboard-light.png`, `dashboard-dark.png`, and `code\` for code snippets) in case the live demo fails.

## 15 minutes before you present

The laptop has 7.7 GB of RAM and the pipeline uses about 3 GB, so close Chrome tabs, Teams and other heavy apps first.

Open PowerShell in the project folder (`C:\Users\admin\OneDrive\Desktop\bda`) and run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\launch.ps1 -Skip 1220000
```

This does the same as the **Trending Hashtags** desktop shortcut, and also starts the replay in mid-June 2009.

- **What happens:** it starts the supervisor, which brings up HDFS, ZooKeeper, Kafka and the four pipeline stages,
  each in its own window. It then opens the dashboard. This takes about 1 to 2 minutes from a cold start.
- **What `-Skip 1220000` does:** the replay starts on 15 June 2009, where `#iranelection` (the Iranian election protests),
  `#iphone` (the iPhone 3GS launch) and `#squarespace` trend. That's a better story than April, where `#followfriday` dominates.
  The producer takes about 20 seconds to skip ahead before tweets start flowing.
  Without `-Skip`, which is what the desktop shortcut does, the producer resumes where it last stopped.
- **Wait about 5 minutes before presenting**, so the 5-minute window is full and the trend lines have a few points.
- **Check the dashboard header:** it should say "Live", "All 8 services running" and "Producer on · 500/s".

**Windows you should see:** 2 for HDFS (namenode, datanode), ZooKeeper, Kafka, HDFS sink, Trend job, Dashboard,
Tweet producer, and a minimised Supervisor. Don't close them. Minimise them, and arrange the producer, sink and
trend job windows so you can show them.

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
6. **Optional, the Producer switch.** Turn it off: the producer window prints "paused from the dashboard", and over the
   next few runs the counts fall as old tweets slide out of the 5-minute window. Turn it back on to resume.
   This shows the sliding window working. Don't leave it off before your audience sees a full leaderboard.
7. **Optional, self-healing.** Close the HDFS sink window. Within about 10 seconds the supervisor reopens it,
   and the header briefly shows "Restarting HDFS sink…".
8. **Code, if asked.** Open `mapreduce\mapper.py`, `combiner.py` and `reducer.py`. Each is about 20 to 40 lines.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Dashboard says "Waiting for data" or "Stale" | Wait for the next run (see the countdown). If it stays stale, look at the Trend job window for an error. |
| Leaderboard says "No hashtags in the last 5 minutes" | Check the Producer switch is on. If it was just started, wait for the 20-second skip to finish. |
| Header says "Restarting …" | The supervisor is restarting that component. Give it 10 to 40 seconds. |
| A window closed by accident | Do nothing: the supervisor reopens it within about 10 seconds. |
| Kafka window closes straight away | Normal after an unclean shutdown. The supervisor retries until it starts. |
| Sink window says "HDFS write failed, retrying" | HDFS is still starting (safe mode). It recovers by itself, and nothing is lost. |
| Browser can't reach localhost:5000 | The supervisor isn't running. Run `scripts\launch.ps1` or double-click the desktop shortcut. |
| Everything is broken | `scripts\stop_all.ps1 -IncludeHdfs`, then `scripts\launch.ps1 -Skip 1220000`. That takes about 2 minutes. If there's no time, use the backup screenshots. |
| Laptop went to sleep | Wake it. The supervisor restarts anything that stopped, and the dashboard catches up within a minute or two. |

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
| Recovery after a crash | about 10 seconds (supervisor checks every 10 seconds) |
| Cold start | 1 to 2 minutes to the first leaderboard |
