#!/bin/bash

echo "=== コンテナの状態確認 ==="
docker-compose ps

echo -e "\n=== Hiveデータベース確認 ==="
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SHOW DATABASES; USE test_db_20250407; SHOW TABLES;"

echo -e "\n=== Hiveテーブルデータ確認 ==="
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.employee_data;"

echo -e "\n=== MySQLメタストア確認 ==="
docker exec mysql_20250407 mysql -u root -proot -e "SHOW DATABASES; USE metastore_db; SHOW TABLES;"

echo -e "\n=== ディレクトリ構造確認 ==="
find ./data -type f | sort
find ./hive-data -type f | sort

echo -e "\n=== Trinoカタログ確認 ==="
docker exec trino_20250407 trino --server localhost:8080 --catalog hive --execute "SHOW SCHEMAS" || echo "Trino not available"
