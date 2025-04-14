#!/bin/bash

# ディレクトリ構造
BASE_DIR=$(pwd)

echo "Running Glue job..."

# Glueコンテナが実行中か確認
GLUE_CONTAINER=$(docker ps -q -f name=glue20250407 -f status=running)
if [ -z "$GLUE_CONTAINER" ]; then
    echo "Glue container is not running. Starting it..."
    docker-compose up -d glue-local
    sleep 10
fi

# テストデータベースの存在を確認
docker exec mysql_20250407 mysql -u root -proot -e "CREATE DATABASE IF NOT EXISTS test_db_20250407;"
docker exec mysql_20250407 mysql -u root -proot -e "GRANT ALL PRIVILEGES ON test_db_20250407.* TO 'hive'@'%';"
docker exec mysql_20250407 mysql -u root -proot -e "FLUSH PRIVILEGES;"

# Hiveメタストアの状態を確認
docker exec hive_metastore_20250407 bash -c "${HIVE_HOME}/bin/hive -e 'SHOW DATABASES;'" || echo "Could not connect to Hive metastore, but continuing..."

# テストデータが存在するか確認
TEST_DATA=$(docker exec hive_metastore_20250407 bash -c "${HIVE_HOME}/bin/hive -e 'SELECT COUNT(*) FROM test_db_20250407.test_table;'" 2>/dev/null)
if [ -z "$TEST_DATA" ]; then
    echo "テストデータが存在しません。テーブルを作成します..."
    docker cp ./temp_ddl.hql hive_metastore_20250407:/opt/hive/temp_ddl.hql
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -f /opt/hive/temp_ddl.hql || echo "Failed to create table, but continuing..."
fi

# Glueジョブの実行
echo "Executing Glue job..."
# 修正：--JOB_NAME パラメータを追加
docker exec glue20250407 bash -c "cd /home/glue_user/workspace && python3 /home/glue_user/workspace/jobs/test_job_hive.py --JOB_NAME local_test_job --database test_db_20250407 --table test_table" || echo "Failed to execute Glue job, but continuing..."

echo "Job execution completed."

# 結果の確認
if [ -d "./data/output/processed_data" ]; then
    echo "Processed data available at: ./data/output/processed_data"
    echo "Data sample:"
    ls -la ./data/output/processed_data
fi

# Hiveでデータを確認
echo "Verifying data in Hive..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.test_table LIMIT 10;" || echo "Failed to query data, but continuing..."

echo "Job verification completed."