#!/bin/bash

# 環境変数の設定
export AWS_REGION=${AWS_REGION:-ap-northeast-1}

echo "Starting local Glue environment with Hive Metastore..."
docker-compose up -d

echo "Waiting for containers to start..."
sleep 20

echo "Environment is ready. You can now use the following commands:"
echo "  - ./scripts/run_crawler.sh     : Run crawler to scan data"
echo "  - ./scripts/run_job.sh         : Run Glue job"
echo "  - ./scripts/execute_sql.sh     : Execute SQL query"
