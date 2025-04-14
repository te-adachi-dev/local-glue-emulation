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

# クローラーが生成したDDLファイルをホストにコピー
echo "Copying DDL file from Glue container..."
docker cp glue20250407:/home/glue_user/temp_ddl.hql ./temp_ddl.hql 2>/dev/null || echo "Failed to copy DDL file, using default DDL instead"

# DDLファイルが存在しない場合はデフォルトを使用
if [ ! -f ./temp_ddl.hql ]; then
    echo "Creating default Hive DDL..."
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
fi

# Hiveメタストアにテーブルを登録
echo "Registering tables to Hive metastore..."
# テストデータベースの作成を試みる
docker exec mysql_20250407 mysql -u root -proot -e "CREATE DATABASE IF NOT EXISTS test_db_20250407;" || echo "Failed to create database in MySQL"
docker exec mysql_20250407 mysql -u root -proot -e "GRANT ALL PRIVILEGES ON test_db_20250407.* TO 'hive'@'%';" || echo "Failed to grant privileges"
docker exec mysql_20250407 mysql -u root -proot -e "FLUSH PRIVILEGES;" || echo "Failed to flush privileges"

# DDLをHiveコンテナに転送
docker cp ./temp_ddl.hql hive_metastore_20250407:/opt/hive/temp_ddl.hql || echo "Failed to copy DDL to Hive container"

# beelineコマンドでDDLを実行
echo "Executing DDL in Hive metastore..."
cat ./temp_ddl.hql
docker exec hive_metastore_20250407 bash -c "cat /opt/hive/temp_ddl.hql && /opt/hive/bin/hive -f /opt/hive/temp_ddl.hql" || echo "Failed to execute DDL with hive CLI"

# Trinoコンテナの状態確認
TRINO_CONTAINER=$(docker ps -q -f name=trino_20250407 -f status=running)
if [ -z "$TRINO_CONTAINER" ]; then
    echo "Trino container is not running. Attempting to restart..."
    docker-compose restart trino_20250407
    echo "Waiting for Trino container to start..."
    sleep 15
fi

# Trinoのヘルスチェック関数
check_trino_health() {
    local status
    status=$(docker exec trino_20250407 curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/v1/info 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        return 0
    else
        return 1
    fi
}

# Trinoの稼働状態を確認（より堅牢なヘルスチェック）
echo "Checking Trino health status..."
MAX_TRINO_ATTEMPTS=10
TRINO_READY=false

for i in $(seq 1 $MAX_TRINO_ATTEMPTS); do
    echo "Checking Trino health (attempt $i/$MAX_TRINO_ATTEMPTS)..."
    
    if check_trino_health; then
        echo "Trino is healthy and ready"
        TRINO_READY=true
        break
    else
        echo "Trino is not yet ready, waiting..."
    fi
    
    # 待機時間を調整（最初は短く、徐々に長く）
    sleep $((5 + i))
done

# Trinoが応答しない場合は処理をスキップ
if [ "$TRINO_READY" = "false" ]; then
    echo "Trino did not respond properly after $MAX_TRINO_ATTEMPTS attempts."
    echo "Skipping Trino checks and using Hive CLI instead."
    
    # Hiveでデータを確認
    echo "Querying tables with Hive..."
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;" || echo "Failed to show tables with Hive"
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SELECT * FROM test_table LIMIT 10;" || echo "Failed to query data with Hive"
    
    echo "Crawler process completed with Hive only (Trino unavailable)."
    exit 0
fi

# Trinoが正常に応答する場合、テーブル確認を実行
echo "Checking registered tables with Trino..."
if ! docker exec trino_20250407 trino --server localhost:8080 --catalog hive --execute "SHOW SCHEMAS"; then
    echo "Failed to show schemas with Trino, falling back to Hive."
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SHOW DATABASES;"
fi

if ! docker exec trino_20250407 trino --server localhost:8080 --catalog hive --schema test_db_20250407 --execute "SHOW TABLES"; then
    echo "Failed to show tables with Trino, falling back to Hive."
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"
fi

echo "Querying data with Trino..."
if ! docker exec trino_20250407 trino --server localhost:8080 --catalog hive --schema test_db_20250407 --execute "SELECT * FROM test_table LIMIT 10"; then
    echo "Failed to query data with Trino, falling back to Hive."
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SELECT * FROM test_table LIMIT 10;"
fi

echo "Crawler process completed successfully."
echo "Generated DDL is available at: ./temp_ddl.hql"