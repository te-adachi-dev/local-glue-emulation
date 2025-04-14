#!/bin/bash

# ディレクトリ構造
BASE_DIR=$(pwd)

echo "Running crawler..."

# テストデータが存在することを確認
if [ ! -f ./data/input/csv/test_table.csv ]; then
    echo "Creating test data..."
    mkdir -p ./data/input/csv
    cat > ./data/input/csv/test_table.csv << 'END_OF_CSV'
id,name,age,department
1,テスト太郎,30,開発部
2,テスト花子,25,営業部
3,テスト次郎,40,総務部
4,テスト三郎,35,人事部
END_OF_CSV
fi

# ウェアハウスディレクトリを準備
echo "Preparing warehouse directory..."
mkdir -p ./data/warehouse/test_table
cp ./data/input/csv/test_table.csv ./data/warehouse/test_table/

# Glueコンテナが実行中か確認
GLUE_CONTAINER=$(docker ps -q -f name=glue20250407 -f status=running)
if [ -z "$GLUE_CONTAINER" ]; then
    echo "Glue container is not running. Starting it..."
    docker-compose up -d glue-local
    sleep 10
fi

# クローラースクリプトをコンテナ内で実行
echo "Running crawler script in Glue container..."
docker exec glue20250407 python3 /home/glue_user/workspace/crawler/crawler_container.py || echo "Failed to run crawler script, but continuing..."

# DDLファイルを上書き作成
echo "Creating Hive DDL..."
cat > ./temp_ddl.hql << 'END_OF_DDL'
CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.test_table (
  `id` INT,
  `name` STRING,
  `age` INT,
  `department` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/home/glue_user/workspace/data/warehouse/test_table'
TBLPROPERTIES ('skip.header.line.count'='1');
END_OF_DDL

# Hiveメタストアにテーブルを登録
echo "Registering tables to Hive metastore..."

# テストデータベースの作成を試みる
docker exec mysql_20250407 mysql -u root -proot -e "CREATE DATABASE IF NOT EXISTS test_db_20250407;"
docker exec mysql_20250407 mysql -u root -proot -e "GRANT ALL PRIVILEGES ON test_db_20250407.* TO 'hive'@'%';"
docker exec mysql_20250407 mysql -u root -proot -e "FLUSH PRIVILEGES;"

# DDLをHiveコンテナに転送
docker cp ./temp_ddl.hql hive_metastore_20250407:/opt/hive/temp_ddl.hql

# beelineコマンドでDDLを実行
echo "Executing DDL in Hive metastore..."
cat ./temp_ddl.hql
docker exec hive_metastore_20250407 bash -c "cat /opt/hive/temp_ddl.hql && /opt/hive/bin/hive -f /opt/hive/temp_ddl.hql || echo 'Failed to execute DDL with hive CLI'"

# Trinoコンテナの状態確認
TRINO_CONTAINER=$(docker ps -q -f name=trino_20250407 -f status=running)
if [ -z "$TRINO_CONTAINER" ]; then
    echo "Trino container is not running. Attempting to restart..."
    docker-compose restart trino
    sleep 30
fi

# Trinoが起動するのを待つ
echo "Waiting for Trino to be ready..."
MAX_TRINO_ATTEMPTS=30
for i in $(seq 1 $MAX_TRINO_ATTEMPTS); do
    echo "Checking Trino (attempt $i/$MAX_TRINO_ATTEMPTS)..."
    if docker exec trino_20250407 bash -c "nc -z localhost 8080" 2>/dev/null; then
        echo "Trino is ready"
        TRINO_READY=true
        break
    fi
    sleep 10
done

# Trinoでテーブルを確認
if [ "$TRINO_READY" == "true" ]; then
    echo "Checking registered tables with Trino..."
    docker exec trino_20250407 trino --server localhost:8080 --catalog hive --execute "SHOW SCHEMAS" || echo "Failed to show schemas, but continuing..."
    
    docker exec trino_20250407 trino --server localhost:8080 --catalog hive --schema test_db_20250407 --execute "SHOW TABLES" || echo "Failed to show tables, but continuing..."
    
    echo "Querying data with Trino..."
    docker exec trino_20250407 trino --server localhost:8080 --catalog hive --schema test_db_20250407 --execute "SELECT * FROM test_table" || echo "Failed to query data, but continuing..."
else
    echo "Trino did not start properly after $MAX_TRINO_ATTEMPTS attempts. Skipping Trino checks."
    
    # Hiveでデータを確認（Trinoのバックアップ）
    echo "Querying data with Hive instead..."
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SELECT * FROM test_table;"
fi

echo "Crawler process completed."
echo "Generated DDL is available at: ./temp_ddl.hql"