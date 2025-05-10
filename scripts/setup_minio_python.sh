#!/bin/bash
# MinIOの初期セットアップスクリプト (Pythonバージョン)

set -e

echo "=== MinIO初期セットアップスクリプト (Pythonバージョン) ==="

# 変数設定
CONTAINER_NAME="glue20250407"
BUCKET_NAME="glue-bucket"

# カレントディレクトリ取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SIMPLE_MINIO_CLIENT="${DIR}/simple_minio_client.py"

# MinIOクライアントスクリプトをコンテナにコピー
echo "MinIOクライアントスクリプトをコンテナにコピー..."
docker cp "${SIMPLE_MINIO_CLIENT}" "${CONTAINER_NAME}:/tmp/simple_minio_client.py"
docker exec $CONTAINER_NAME chmod +x /tmp/simple_minio_client.py

# MinIOが起動するまで待機
echo "MinIOサーバーの起動を待機しています..."
MAX_RETRIES=30
RETRY_COUNT=0

while ! docker exec $CONTAINER_NAME curl --output /dev/null --silent --head --fail http://minio:9000/minio/health/live; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -gt $MAX_RETRIES ]; then
        echo "エラー: MinIOの起動待機がタイムアウトしました"
        exit 1
    fi
    echo -n "."
    sleep 2
done
echo ""
echo "MinIOサーバーが起動しました"

# バケットの作成
echo "バケット '${BUCKET_NAME}' を作成しています..."
docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
            -e AWS_SECRET_ACCESS_KEY=minioadmin \
            $CONTAINER_NAME python3 /tmp/simple_minio_client.py create_bucket ${BUCKET_NAME}

# 必要なフォルダ構造を作成
echo "フォルダ構造を作成しています..."

# 各種ディレクトリを作成
for path in \
    "input/" \
    "input/csv/" \
    "master/" \
    "master/device_attribute/" \
    "master/status/" \
    "master/user_attribute/" \
    "output/" \
    "output/list_ap_output/" \
    "output/list_ap_output/list_ap_backup/" \
    "output/list_ap_output/list_ap_table/" \
    "output/list_ap_output/common_ap/" \
    "output/connection_output/"; do
    docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
                -e AWS_SECRET_ACCESS_KEY=minioadmin \
                $CONTAINER_NAME python3 /tmp/simple_minio_client.py put_object ${BUCKET_NAME} ${path} ""
    echo "  ディレクトリ作成: ${path}"
done

echo "MinIOのバケット内のオブジェクト一覧を確認しています..."
docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
            -e AWS_SECRET_ACCESS_KEY=minioadmin \
            $CONTAINER_NAME python3 /tmp/simple_minio_client.py list_objects ${BUCKET_NAME}

echo "MinIOの初期セットアップが完了しました"
echo "MinIO管理コンソール: http://localhost:9001"
echo "  ユーザー名: minioadmin"
echo "  パスワード: minioadmin"
