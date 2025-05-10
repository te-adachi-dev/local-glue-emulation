#!/bin/bash
# MinIOクライアントラッパースクリプト

set -e

# コンテナ名
CONTAINER_NAME="glue20250407"

# 引数のチェック
if [ $# -lt 1 ]; then
    echo "使用方法: $0 <command> [args...]"
    echo "コマンド:"
    echo "  list_buckets - バケット一覧を表示"
    echo "  create_bucket <bucket_name> - バケット作成"
    echo "  put_object <bucket_name> <key> <content> - オブジェクト作成"
    echo "  list_objects <bucket_name> [prefix] - オブジェクト一覧"
    echo "  copy_file <local_path> <bucket_name> <key> - ファイルをアップロード"
    exit 1
fi

# カレントディレクトリ取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SIMPLE_MINIO_CLIENT="${DIR}/simple_minio_client.py"

# MinIOクライアントスクリプトをコンテナにコピー（存在しない場合）
if ! docker exec $CONTAINER_NAME test -f /tmp/simple_minio_client.py; then
    echo "MinIOクライアントスクリプトをコンテナにコピー..."
    docker cp "${SIMPLE_MINIO_CLIENT}" "${CONTAINER_NAME}:/tmp/simple_minio_client.py"
    docker exec -u root $CONTAINER_NAME chmod +x /tmp/simple_minio_client.py
fi

# コマンドとパラメータを取得
command=$1
shift
params=("$@")

# コマンド実行
docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
            -e AWS_SECRET_ACCESS_KEY=minioadmin \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            $CONTAINER_NAME python3 /tmp/simple_minio_client.py ${command} ${params[@]}
