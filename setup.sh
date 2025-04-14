#!/bin/bash

echo "=== 環境セットアップスクリプト ==="

# 1. テストデータを準備
echo "1. テストデータを準備中..."
mkdir -p ./data/input/csv
cat > ./data/input/csv/test_table.csv << 'END_OF_CSV'
id,name,age,department
1,テスト太郎,30,開発部
2,テスト花子,25,営業部
3,テスト次郎,40,総務部
4,テスト三郎,35,人事部
END_OF_CSV

# 2. コンテナを停止して削除
echo "2. コンテナを停止して削除中..."
docker-compose down -v
rm -rf mysql-data hive-data trino-data

# 3. Trinoのカタログ設定を修正
echo "3. Trinoのカタログ設定を修正中..."
mkdir -p ./conf/trino-conf/catalog
cat > ./conf/trino-conf/catalog/hive.properties << 'END_OF_PROPERTIES'
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.non-managed-table-writes-enabled=true
hive.collect-column-statistics-on-write=true
END_OF_PROPERTIES

# 4. DDLファイルを作成
echo "4. DDLファイルを作成中..."
cat > ./temp_ddl.hql << 'END_OF_DDL'
CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.test_table (
  id INT,
  name STRING,
  age INT,
  department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/test_db_20250407.db/test_table'
TBLPROPERTIES ('skip.header.line.count'='1');
END_OF_DDL

# 5. データ格納ディレクトリを準備
echo "5. データ格納ディレクトリを準備中..."
mkdir -p ./hive-data/warehouse/test_db_20250407.db/test_table
cp ./data/input/csv/test_table.csv ./hive-data/warehouse/test_db_20250407.db/test_table/

# 6. ディレクトリ権限を修正
echo "6. ディレクトリ権限を修正中..."
chmod -R 777 ./hive-data
chmod -R 777 ./data

# 7. コンテナを起動
echo "7. コンテナを起動中..."
docker-compose up -d

# 8. コンテナの起動待機
echo "8. コンテナの起動待機中..."
sleep 60
echo "環境の準備が完了しました！"
