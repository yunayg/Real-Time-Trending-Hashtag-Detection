"""Shared settings for the trending-hashtag pipeline."""
import getpass
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# Datasets
SENTIMENT140_CSV = os.path.join(ROOT, "sentiment", "training.1600000.processed.noemoticon.csv")
AIRLINE_CSV = os.path.join(ROOT, "airline", "Tweets.csv")

# Kafka
# 127.0.0.1 rather than "localhost": on Windows "localhost" tries IPv6 first and
# costs ~2 seconds per connection when the service only listens on IPv4.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
KAFKA_TOPIC = "tweets"
KAFKA_GROUP = "hdfs-sink"

# HDFS (accessed over WebHDFS so the Python processes need no JVM)
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://127.0.0.1:9870")
HDFS_USER = os.environ.get("HDFS_USER", getpass.getuser())
RAW_DIR = "/tweets/raw"            # raw events: RAW_DIR/YYYYMMDD/HHMM/part-*.jsonl (UTC)
TRENDING_DIR = "/tweets/trending"  # MapReduce outputs and latest.json

# Hadoop install used to launch the MapReduce job
HADOOP_HOME = os.environ.get("HADOOP_HOME", r"C:\hadoop\hadoop-3.3.6")
JAVA_HOME = os.environ.get("JAVA_HOME", r"C:\java\jdk8u504-b01")

# Trend computation
WINDOW_MINUTES = 5
SLIDE_SECONDS = 60
TOP_N = 10

DASHBOARD_PORT = 5000

# Producer control (the dashboard's on/off switch). The producer writes its status to
# PRODUCER_STATE about once a second and pauses while PRODUCER_PAUSE exists.
STATE_DIR = os.path.join(ROOT, "state")
PRODUCER_STATE = os.path.join(STATE_DIR, "producer.json")
PRODUCER_PAUSE = os.path.join(STATE_DIR, "producer.paused")
PRODUCER_RATE = 500
