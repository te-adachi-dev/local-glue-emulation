#!/bin/bash
# AWS CLIとbotocoreのバージョン不整合を修正するスクリプト
set -e

echo "=== AWS CLIとbotocoreのバージョン不整合を修正 ==="

CONTAINER_NAME="glue20250407"

# コンテナ内のpipをアップグレード
echo "pipをアップグレード中..."
docker exec -u root $CONTAINER_NAME pip3 install --upgrade pip

# 現在のバージョンを確認
echo "現在のパッケージバージョンを確認中..."
docker exec $CONTAINER_NAME pip3 list | grep -E "boto|aws"

# まずAWS CLIをアンインストール
echo "AWS CLIをアンインストール中..."
docker exec -u root $CONTAINER_NAME pip3 uninstall -y awscli

# botocoreの特定バージョンをインストール
echo "botocoreの特定バージョンをインストール中..."
docker exec -u root $CONTAINER_NAME pip3 install botocore==1.27.96

# 互換性のあるAWS CLIバージョンをインストール
echo "互換性のあるAWS CLIをインストール中..."
docker exec -u root $CONTAINER_NAME pip3 install awscli==1.25.97

# boto3も同じバージョン系列に揃える
echo "boto3を互換バージョンにダウングレード中..."
docker exec -u root $CONTAINER_NAME pip3 uninstall -y boto3
docker exec -u root $CONTAINER_NAME pip3 install boto3~=1.24.0

# 最終的なバージョンを確認
echo "インストール後のパッケージバージョン:"
docker exec $CONTAINER_NAME pip3 list | grep -E "boto|aws"

echo "バージョン不整合の修正が完了しました。"
