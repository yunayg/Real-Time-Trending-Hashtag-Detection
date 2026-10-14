# Starts the infrastructure (HDFS, ZooKeeper, Kafka) if it is not already running,
# each in its own window, and creates the "tweets" topic.
#   powershell -ExecutionPolicy Bypass -File scripts\start_services.ps1

$HadoopHome = if ($env:HADOOP_HOME) { $env:HADOOP_HOME } else { 'C:\hadoop\hadoop-3.3.6' }
$JavaHome   = if ($env:JAVA_HOME)   { $env:JAVA_HOME }   else { 'C:\java\jdk8u504-b01' }
$KafkaHome  = 'C:\Kafka\kafka_2.12-2.3.1\kafka_2.12-2.3.1'

$env:HADOOP_HOME = $HadoopHome
$env:JAVA_HOME   = $JavaHome
$env:Path        = "$HadoopHome\bin;$HadoopHome\sbin;$JavaHome\bin;$env:Path"
# Kafka 2.3.1's start script calls "wmic" (removed from Windows 11) unless the heap size is preset.
$env:KAFKA_HEAP_OPTS = '-Xmx512M -Xms512M'

function Test-Port($port) {
    [bool](Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue)
}

function Wait-Port($name, $port, $seconds = 60) {
    for ($i = 0; $i -lt $seconds; $i++) {
        if (Test-Port $port) { Write-Host "$name is up (port $port)"; return }
        Start-Sleep -Seconds 1
    }
    throw "$name did not start listening on port $port within $seconds seconds"
}

if (Test-Port 9000) { Write-Host 'HDFS already running' }
else {
    Write-Host 'Starting HDFS...'
    Start-Process cmd -ArgumentList '/k', "`"$HadoopHome\sbin\start-dfs.cmd`""
    Wait-Port 'HDFS NameNode' 9000 90
}
# A freshly started NameNode is read-only (safe mode) until the DataNode reports in.
& "$HadoopHome\bin\hdfs.cmd" dfsadmin -safemode wait

if (Test-Port 2181) { Write-Host 'ZooKeeper already running' }
else {
    Write-Host 'Starting ZooKeeper...'
    Start-Process cmd -WorkingDirectory $KafkaHome -ArgumentList '/k', 'title ZooKeeper && bin\windows\zookeeper-server-start.bat config\zookeeper.properties'
    Wait-Port 'ZooKeeper' 2181
}

if (Test-Port 9092) { Write-Host 'Kafka already running' }
else {
    # If the previous broker was killed rather than shut down, ZooKeeper keeps its registration
    # for a few seconds and the new broker exits with NodeExists. Retry until it sticks.
    foreach ($attempt in 1..3) {
        Write-Host "Starting Kafka (attempt $attempt)..."
        $kafka = Start-Process cmd -WorkingDirectory $KafkaHome -PassThru -ArgumentList '/c', 'title Kafka && bin\windows\kafka-server-start.bat config\server.properties'
        while (-not $kafka.HasExited -and -not (Test-Port 9092)) { Start-Sleep -Seconds 1 }
        if (Test-Port 9092) { Write-Host 'Kafka is up (port 9092)'; break }
        Start-Sleep -Seconds 8
    }
    if (-not (Test-Port 9092)) { throw 'Kafka did not start; see logs\server.log in the Kafka folder' }
}

& "$KafkaHome\bin\windows\kafka-topics.bat" --create --zookeeper localhost:2181 --if-not-exists --topic tweets --partitions 1 --replication-factor 1
Write-Host 'Services ready. Next: scripts\start_pipeline.ps1'
