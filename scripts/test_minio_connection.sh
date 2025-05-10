#!/bin/bash
# MinIOへの接続をテストするスクリプト

set -e

echo "=== MinIO接続テストスクリプト ==="

# 変数設定
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET_NAME="glue-bucket"
CONTAINER_NAME="glue20250407"

# コンテナ内のAWS CLI設定を表示
echo "AWS CLI設定を表示します："
docker exec $CONTAINER_NAME aws configure list

echo "現在のコンテナ環境変数を表示します..."
docker exec $CONTAINER_NAME env | grep AWS

echo "AWS認証情報を環境変数として設定します..."
docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            $CONTAINER_NAME aws configure list

echo "MinIOへの接続をテストします..."
docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            $CONTAINER_NAME aws --endpoint-url=${MINIO_ENDPOINT} s3 ls

echo "バケットを作成します..."
docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            $CONTAINER_NAME aws --endpoint-url=${MINIO_ENDPOINT} s3 mb s3://${BUCKET_NAME} || true

echo "バケット一覧を表示します..."
docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            $CONTAINER_NAME aws --endpoint-url=${MINIO_ENDPOINT} s3 ls

echo "テスト完了"
