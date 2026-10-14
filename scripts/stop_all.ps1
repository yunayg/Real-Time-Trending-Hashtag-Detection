# Stops the pipeline stages, Kafka and ZooKeeper (and HDFS with -IncludeHdfs),
# closing the windows the start scripts opened.
#   powershell -ExecutionPolicy Bypass -File scripts\stop_all.ps1 [-IncludeHdfs]

param([switch]$IncludeHdfs)

function Stop-Matching($label, $pattern) {
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='java.exe'" |
        Where-Object { $_.CommandLine -match $pattern }
    foreach ($p in $procs) {
        $parent = Get-CimInstance Win32_Process -Filter "ProcessId=$($p.ParentProcessId)" -ErrorAction SilentlyContinue
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        # close the console window it ran in
        if ($parent -and $parent.Name -eq 'cmd.exe') { Stop-Process -Id $parent.ProcessId -Force -ErrorAction SilentlyContinue }
    }
    Write-Host ("{0,-12} {1}" -f $label, $(if ($procs) { 'stopped' } else { 'not running' }))
}

# the supervisor first, or it would restart everything we stop
Stop-Matching 'Supervisor' 'supervisor\.py'
# then upstream first, so nothing is left writing into a stopped stage
Stop-Matching 'Producer'  'tweet_producer\.py'
Stop-Matching 'HDFS sink' 'kafka_to_hdfs\.py'
Stop-Matching 'Trend job' 'run_trending_job\.py'
Stop-Matching 'Dashboard' 'dashboard\\app\.py'
Stop-Matching 'Kafka'     'kafka\.Kafka '
Stop-Matching 'ZooKeeper' 'QuorumPeerMain'
if ($IncludeHdfs) {
    Stop-Matching 'DataNode' 'hdfs\.server\.datanode\.DataNode'
    Stop-Matching 'NameNode' 'hdfs\.server\.namenode\.NameNode'
}
