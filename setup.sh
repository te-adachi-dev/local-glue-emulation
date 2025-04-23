#!/bin/bash

echo "=== AWS Glue エミュレータ環境セットアップスクリプト ==="

# エラーが発生したら即終了
set -e

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
echo "   MySQL の起動を待機しています（30秒）..."
sleep 30

# MySQL の起動確認
echo "   MySQL の接続を確認しています..."
MAX_RETRIES=5
RETRY_COUNT=0
while ! docker exec mysql_20250407 mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "   MySQL への接続に失敗しました。セットアップを中止します。"
        exit 1
    fi
    echo "   MySQL はまだ準備ができていません。再試行します..."
    sleep 10
done
echo "   MySQL の準備ができました。"

# Hive Metastore の起動待機
echo "   Hive Metastore の起動を待機しています（30秒）..."
sleep 30

# Hive Metastore の起動確認
echo "   Hive Metastore の接続を確認しています..."
MAX_RETRIES=5
RETRY_COUNT=0
while ! docker exec hive_metastore_20250407 nc -z localhost 9083 >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "   Hive Metastore への接続に失敗しました。セットアップを中止します。"
        exit 1
    fi
    echo "   Hive Metastore はまだ準備ができていません。再試行します..."
    sleep 10
done
echo "   Hive Metastore の準備ができました。"

echo "5. コンテナの状態を確認中..."
docker-compose ps

echo "====================================="
echo "環境のセットアップが完了しました！"
echo "====================================="
echo ""
echo "次のステップ:"
echo "1. データを ./data/input/csv/ に配置してください"
echo "2. ./run_crawler.sh を実行してテーブルを作成してください"
echo "3. Glueジョブを実行して処理を開始してください"
echo ""
echo "詳細な手順については README.md を参照してください"
