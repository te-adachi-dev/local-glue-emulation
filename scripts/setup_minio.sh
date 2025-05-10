#!/bin/bash
# MinIOの初期セットアップスクリプト

set -e

echo "=== MinIO初期セットアップスクリプト ==="

# 変数設定
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET_NAME="glue-bucket"

# AWS CLIがインストールされているかチェック
if ! command -v aws &> /dev/null; then
    echo "AWS CLIをインストールしています..."
    pip install awscli
fi

# ホスト側で実行する場合はlocalhostに変更
if [ "$1" == "local" ]; then
    MINIO_ENDPOINT="http://localhost:9000"
    echo "ローカル実行モード: エンドポイントを $MINIO_ENDPOINT に設定"
fi

# MinIOが起動するまで待機
echo "MinIOサーバーが起動するのを待機しています..."
MAX_RETRIES=30
RETRY_COUNT=0

while ! curl --output /dev/null --silent --head --fail ${MINIO_ENDPOINT}/minio/health/live; do
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

# AWS CLIの設定
export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}"
export AWS_DEFAULT_REGION="ap-northeast-1"

aws configure set aws_access_key_id ${MINIO_ACCESS_KEY}
aws configure set aws_secret_access_key ${MINIO_SECRET_KEY}
aws configure set default.region ap-northeast-1
aws configure set default.s3.signature_version s3v4

# バケットの作成
echo "バケット '${BUCKET_NAME}' を作成しています..."
aws --endpoint-url=${MINIO_ENDPOINT} s3 mb s3://${BUCKET_NAME} 2>/dev/null || true

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
    
    aws --endpoint-url=${MINIO_ENDPOINT} s3api put-object --bucket ${BUCKET_NAME} --key ${path} --content-length 0 2>/dev/null || true
    echo "  ディレクトリ作成: ${path}"
done

echo "MinIOの初期セットアップが完了しました"
echo "MinIO管理コンソール: ${MINIO_ENDPOINT/9000/9001}"
echo "  ユーザー名: ${MINIO_ACCESS_KEY}"
echo "  パスワード: ${MINIO_SECRET_KEY}"
