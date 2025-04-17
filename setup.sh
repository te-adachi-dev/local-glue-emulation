#!/bin/bash

echo "=== 環境セットアップスクリプト ==="

# 1. ディレクトリ構造の準備
echo "1. ディレクトリ構造を準備中..."
mkdir -p ./data/input/csv
mkdir -p ./data/output
mkdir -p ./data/warehouse
mkdir -p ./conf/trino-conf/catalog
mkdir -p ./hive-data/warehouse
mkdir -p ./trino-data

# 2. 権限設定
echo "2. 権限を設定中..."
chmod -R 777 ./data
chmod -R 777 ./hive-data
chmod -R 777 ./trino-data

# 3. コンテナの起動
echo "3. コンテナを起動中..."
docker-compose up -d

# 4. 起動完了待機
echo "4. 環境の起動を待機中..."
echo "環境のセットアップが完了しました。"
echo "データを ./data/input/csv/ に配置し、クローラを実行してください。"