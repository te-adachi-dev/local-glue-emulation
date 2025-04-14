#!/bin/bash

# CSVファイル名
CSV_NAME="wifi_access_log"

# CSVファイルを作成
echo "Creating new CSV file..."
cat > ./data/input/csv/${CSV_NAME}.csv << 'END_OF_CSV'
mac_address,connection_start,connection_end,access_point_id,signal_strength
00:1A:2B:3C:4D:5E,2025-04-15 10:00:00,2025-04-15 10:30:00,AP_001,-65 dBm
00:2B:3C:4D:5E:6F,2025-04-15 10:05:00,2025-04-15 10:45:00,AP_002,-70 dBm
00:3C:4D:5E:6F:7A,2025-04-15 10:10:00,2025-04-15 11:00:00,AP_001,-60 dBm
END_OF_CSV

# ウェアハウスディレクトリを準備
echo "Preparing warehouse directory..."
mkdir -p ./data/warehouse/${CSV_NAME}
cp ./data/input/csv/${CSV_NAME}.csv ./data/warehouse/${CSV_NAME}/

# クローラーを実行
echo "Running crawler..."
./scripts/run_crawler.sh

# テーブルとデータの確認
echo "Checking tables..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"

echo "Querying new data..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.${CSV_NAME};"

echo "Process completed successfully."
