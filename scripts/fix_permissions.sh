#!/bin/bash
# コンテナ内の権限問題を修正するスクリプト

set -e

echo "=== コンテナ内の権限問題を修正しています ==="

CONTAINER_NAME="glue20250407"

# ディレクトリの権限を修正
docker exec -u root $CONTAINER_NAME bash -c "chmod -R 777 /home/glue_user/workspace"
docker exec -u root $CONTAINER_NAME bash -c "chmod -R 777 /home/glue_user/data"
docker exec -u root $CONTAINER_NAME bash -c "chmod -R 777 /home/glue_user/crawler"

# boto3とawscliをインストール
docker exec -u root $CONTAINER_NAME bash -c "pip3 install boto3 awscli"

# 環境変数を確認
echo "コンテナ内の環境変数を確認します..."
docker exec $CONTAINER_NAME env | grep AWS

# AWS認証情報を設定
echo "コンテナ内のAWS認証情報を設定しています..."
docker exec $CONTAINER_NAME bash -c "aws configure set aws_access_key_id minioadmin"
docker exec $CONTAINER_NAME bash -c "aws configure set aws_secret_access_key minioadmin"
docker exec $CONTAINER_NAME bash -c "aws configure set default.region ap-northeast-1"
docker exec $CONTAINER_NAME bash -c "aws configure set default.s3.signature_version s3v4"
docker exec $CONTAINER_NAME bash -c "aws configure set default.s3.addressing_style path"

# 設定を確認
echo "設定を確認しています..."
docker exec $CONTAINER_NAME aws configure list

echo "修正が完了しました。"
