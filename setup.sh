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
mkdir -p ./trino-data/etc
mkdir -p ./trino-data/plugin
mkdir -p ./trino-data/secrets-plugin
mkdir -p ./trino-data/var

# 2. 権限設定
echo "2. 権限を設定中..."
chmod -R 777 ./data
chmod -R 777 ./hive-data
chmod -R 777 ./trino-data

# 3. 既存のコンテナを停止・削除
echo "3. 既存のコンテナを確認・停止中..."
if [ "$(docker ps -a -q -f name=mysql_20250407)" ]; then
    echo "   既存のコンテナを停止・削除しています..."
    docker-compose down
    sleep 5
fi

# 4. コンテナの起動
echo "4. コンテナを起動中..."
docker-compose up -d

# 5. 起動完了待機
echo "5. 環境の起動を待機中..."
echo "   MySQL の起動を待機しています（60秒）..."
sleep 60

# MySQL の起動確認
echo "   MySQL の接続を確認しています..."
MAX_RETRIES=10
RETRY_COUNT=0
while ! docker exec mysql_20250407 mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "   MySQL への接続に失敗しました。MySQLコンテナのログを確認します..."
        docker logs mysql_20250407
        echo "   セットアップを中止します。"
        exit 1
    fi
    echo "   MySQL はまだ準備ができていません。再試行します...(${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 15
done
echo "   MySQL の準備ができました。"

# Hive Metastore の起動待機
echo "   Hive Metastore の起動を待機しています（60秒）..."
sleep 60

# Hive Metastore の起動確認
echo "   Hive Metastore の接続を確認しています..."
MAX_RETRIES=10
RETRY_COUNT=0
while ! docker exec hive_metastore_20250407 nc -z localhost 9083 >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "   Hive Metastore への接続に失敗しました。Hiveコンテナのログを確認します..."
        docker logs hive_metastore_20250407
        echo "   セットアップを中止します。"
        exit 1
    fi
    echo "   Hive Metastore はまだ準備ができていません。再試行します...(${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 15
done
echo "   Hive Metastore の準備ができました。"

echo "6. コンテナの状態を確認中..."
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
