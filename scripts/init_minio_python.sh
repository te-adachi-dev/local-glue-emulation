#!/bin/bash
# MinIO環境の一括セットアップスクリプト (Pythonバージョン)
# AWS Glueエミュレーション環境をMinIOで構築する

set -e

# セットアップ開始ログ
echo "=== MinIO対応Glueエミュレーション環境セットアップ (Pythonバージョン) ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - セットアップ開始"

# ディレクトリ設定
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$DIR/.."

# 必要なディレクトリを作成
echo "ディレクトリを作成します..."
mkdir -p "${BASE_DIR}/minio-data"
mkdir -p "${BASE_DIR}/data/input/csv"
mkdir -p "${BASE_DIR}/data/output/list_ap_output/list_ap_table"
mkdir -p "${BASE_DIR}/data/output/list_ap_output/list_ap_backup"
mkdir -p "${BASE_DIR}/data/output/list_ap_output/common_ap"
mkdir -p "${BASE_DIR}/data/output/connection_output"

# BUCKET_NAME.pyファイルの作成（存在しない場合）
BUCKET_NAME_PY="${BASE_DIR}/jobs/BUCKET_NAME.py"
if [ ! -f "$BUCKET_NAME_PY" ]; then
    echo "BUCKET_NAME.py ファイルを作成します..."
    cat > "$BUCKET_NAME_PY" << EOL
# 本番環境のバケット名
OPEN = "ap-open-bucket"       # 外部公開データ用バケット
MIDDLE = "ap-middle-bucket"   # 中間データ用バケット
MASTER = "ap-master-bucket"   # マスターデータ用バケット
INITIAL = "ap-initial-bucket" # 初期データ用バケット

# ローカル環境のバケット名（MinIO）
LOCAL_BUCKET = "glue-bucket"

# マスターデータのパス（本番環境）
MASTER_DEVICE_PATH = "device_attribute/"
MASTER_STATUS_PATH = "status/"
MASTER_USER_PATH = "user_attribute/"

# 入力データのパス（本番環境）
INITIAL_BASE_PATH = "input/input_connection/"

# 出力データのパス（本番環境）
MIDDLE_BASE_PATH = "common/common_connection/"
EOL
    echo "BUCKET_NAME.py ファイルを作成しました: $BUCKET_NAME_PY"
fi

# MinIOクライアントスクリプトを作成
SIMPLE_MINIO_CLIENT="${BASE_DIR}/scripts/simple_minio_client.py"
if [ ! -f "$SIMPLE_MINIO_CLIENT" ]; then
    echo "MinIOクライアントスクリプトを作成します..."
    cat > "$SIMPLE_MINIO_CLIENT" << 'EOL'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# MinIOとの通信を行うシンプルなスクリプト

import boto3
import sys
import os
from botocore.client import Config

def main():
    # コマンドライン引数の解析
    if len(sys.argv) < 2:
        print("使用方法: python3 simple_minio_client.py [command] [args...]")
        print("コマンド:")
        print("  list_buckets - バケット一覧を表示")
        print("  create_bucket [bucket_name] - バケット作成")
        print("  put_object [bucket_name] [key] [content] - オブジェクト作成")
        print("  list_objects [bucket_name] [prefix] - オブジェクト一覧")
        print("  copy_file [local_path] [bucket_name] [key] - ファイルをアップロード")
        sys.exit(1)

    # MinIO設定
    endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    # S3クライアント初期化
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='ap-northeast-1'
        )
        s3_client.list_buckets()
        print(f"MinIO接続成功: {endpoint_url}")
    except Exception as e:
        print(f"MinIO接続エラー: {e}")
        sys.exit(1)

    # コマンド処理
    command = sys.argv[1]

    if command == "list_buckets":
        response = s3_client.list_buckets()
        print("バケット一覧:")
        for bucket in response['Buckets']:
            print(f"  {bucket['Name']} (作成日時: {bucket['CreationDate']})")

    elif command == "create_bucket":
        if len(sys.argv) < 3:
            print("エラー: バケット名を指定してください")
            sys.exit(1)
        bucket_name = sys.argv[2]
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"バケット '{bucket_name}' を作成しました")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e):
                print(f"バケット '{bucket_name}' は既に存在しています")
            else:
                print(f"バケット作成エラー: {e}")

    elif command == "put_object":
        if len(sys.argv) < 5:
            print("エラー: バケット名、キー、内容を指定してください")
            sys.exit(1)
        bucket_name, key, content = sys.argv[2:5]
        try:
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)
            print(f"オブジェクトを作成しました: s3://{bucket_name}/{key}")
        except Exception as e:
            print(f"オブジェクト作成エラー: {e}")

    elif command == "list_objects":
        if len(sys.argv) < 3:
            print("エラー: バケット名を指定してください")
            sys.exit(1)
        bucket_name = sys.argv[2]
        prefix = sys.argv[3] if len(sys.argv) > 3 else ""
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' in response:
                print(f"バケット '{bucket_name}' のオブジェクト一覧:")
                for obj in response['Contents']:
                    print(f"  {obj['Key']} (サイズ: {obj['Size']} バイト)")
            else:
                print(f"バケット '{bucket_name}' にオブジェクトがありません")
        except Exception as e:
            print(f"オブジェクト一覧取得エラー: {e}")

    elif command == "copy_file":
        if len(sys.argv) < 4:
            print("エラー: ローカルパス、バケット名、キーを指定してください")
            sys.exit(1)
        local_path = sys.argv[2]
        bucket_name = sys.argv[3]
        key = sys.argv[4] if len(sys.argv) > 4 else os.path.basename(local_path)
        try:
            with open(local_path, 'rb') as file:
                s3_client.upload_fileobj(file, bucket_name, key)
            print(f"ファイルをアップロードしました: {local_path} -> s3://{bucket_name}/{key}")
        except Exception as e:
            print(f"ファイルアップロードエラー: {e}")

    else:
        print(f"エラー: 不明なコマンド '{command}'")
        sys.exit(1)

if __name__ == "__main__":
    main()
EOL
    chmod +x "$SIMPLE_MINIO_CLIENT"
    echo "MinIOクライアントスクリプトを作成しました: $SIMPLE_MINIO_CLIENT"
fi

# スクリプトファイルに実行権限を付与
echo "スクリプトファイルに実行権限を付与します..."
chmod +x "${BASE_DIR}/scripts/"*.{sh,py}

# Docker Composeでコンテナを起動
echo "Docker Composeでコンテナを起動します..."
cd "${BASE_DIR}"
docker-compose up -d

# コンテナ内での権限設定
echo "コンテナ内の権限を設定しています..."
CONTAINER_NAME="glue20250407"
docker exec -u root "$CONTAINER_NAME" bash -c "chmod -R 777 /home/glue_user/workspace"
docker exec -u root "$CONTAINER_NAME" bash -c "chmod -R 777 /home/glue_user/data"
docker exec -u root "$CONTAINER_NAME" bash -c "mkdir -p /home/glue_user/.aws"
docker exec -u root "$CONTAINER_NAME" bash -c "echo '[default]' > /home/glue_user/.aws/credentials"
docker exec -u root "$CONTAINER_NAME" bash -c "echo 'aws_access_key_id = minioadmin' >> /home/glue_user/.aws/credentials"
docker exec -u root "$CONTAINER_NAME" bash -c "echo 'aws_secret_access_key = minioadmin' >> /home/glue_user/.aws/credentials"
docker exec -u root "$CONTAINER_NAME" bash -c "chmod -R 777 /home/glue_user/.aws"


# MinIOの初期設定
echo "MinIOの初期設定を実行します..."
"${BASE_DIR}/scripts/setup_minio_python.sh"

# CSVファイルのアップロード
echo "CSVファイルをMinIOにアップロードします..."
"${BASE_DIR}/scripts/upload_csvs_python.sh" "${BASE_DIR}/data/input/csv"

# セットアップ完了ログ
echo "$(date '+%Y-%m-%d %H:%M:%S') - セットアップ完了"
echo ""
echo "MinIO対応のGlueジョブを実行するには以下のコマンドを使用してください："
echo "  - コンテナ内で実行：./scripts/run_minio_container_job_python.sh"
echo ""
echo "MinIO管理コンソール："
echo "  URL: http://localhost:9001"
echo "  ユーザー: minioadmin"
echo "  パスワード: minioadmin"
