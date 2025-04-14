#!/bin/bash

echo "=== データ検証スクリプト ==="

# 1. Hiveでデータベース一覧を表示
echo "1. Hiveでデータベース一覧を表示..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SHOW DATABASES;"

# 2. Hiveでテーブル作成
echo "2. Hiveでテーブル作成..."
docker cp ./temp_ddl.hql hive_metastore_20250407:/opt/hive/temp_ddl.hql
docker exec hive_metastore_20250407 /opt/hive/bin/hive -f /opt/hive/temp_ddl.hql

# 3. テーブル一覧表示
echo "3. テーブル一覧表示..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"

# 4. データ確認
echo "4. データ確認..."
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.test_table;"

echo "データ検証が完了しました！"
