#!/bin/bash
set -e

ENV=$1               # uat or prod
COLOR=$2             # blue or green
CONFIG_DIR="/opt/flink-configs/$ENV"
JAR="/opt/deploy/flink/all-1.2.2.jar"

# Build image locally (optional)
docker build -t flink-job:latest /opt/deploy/flink

# Stop old color if exists
docker rm -f flink-jobmanager-$COLOR flink-taskmanager-$COLOR || true

# Start JobManager
docker run -d \
  --name flink-jobmanager-$COLOR \
  -p $([ "$COLOR" = "blue" ] && echo "8081:8081" || echo "8082:8081") \
  -v $CONFIG_DIR:/opt/flink/configs:ro \
  flink-job:latest jobmanager

# Start TaskManager
docker run -d \
  --name flink-taskmanager-$COLOR \
  --link flink-jobmanager-$COLOR \
  flink-job:latest taskmanager

# Wait 5 minutes for stability
echo "Waiting 5 minutes for job health..."
sleep 300

# Start all jobs per config file
for cfg in $CONFIG_DIR/config-*; do
  echo "Starting Flink job for config $cfg"
  docker exec flink-jobmanager-$COLOR \
    flink run -d "$JAR" "$cfg"
done

# Check if JobManager is running
if ! docker ps | grep -q flink-jobmanager-$COLOR; then
  echo "Deployment failed: JobManager crashed"
  exit 1
fi

echo "Deployment successful!"
