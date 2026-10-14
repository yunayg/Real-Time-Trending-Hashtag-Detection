# Starts the four pipeline stages, each in its own window, and opens the dashboard.
# Run scripts\start_services.ps1 first. Close a window (or press Ctrl+C in it) to stop that stage.
#   powershell -ExecutionPolicy Bypass -File scripts\start_pipeline.ps1 [-Rate 500] [-Skip 0]
# -Skip N starts the replay N tweets in (the dataset runs April to June 2009).

param([int]$Rate = 500, [int]$Skip = 0)

$Root = Split-Path -Parent $PSScriptRoot

function Start-Stage($title, $command) {
    Start-Process cmd -WorkingDirectory $Root -ArgumentList '/k', "title $title && $command"
}

Start-Stage 'HDFS sink (Kafka to HDFS)'   'python -u consumer\kafka_to_hdfs.py'
Start-Stage 'Trend job (MapReduce)'       'python -u scripts\run_trending_job.py'
Start-Stage 'Dashboard'                   'python -u dashboard\app.py'
Start-Sleep -Seconds 5   # let the sink join the topic before tweets start flowing
Start-Stage 'Tweet producer'              "python -u producer\tweet_producer.py --rate $Rate --skip $Skip --loop"

Start-Sleep -Seconds 3
Start-Process 'http://localhost:5000'
