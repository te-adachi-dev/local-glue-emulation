#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# MinIOとの通信を行うシンプルなスクリプト

import boto3
import sys
import os
from botocore.client import Config

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
endpoint_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"

# 引数から環境変数を優先
if "MINIO_ENDPOINT" in os.environ:
    endpoint_url = os.environ["MINIO_ENDPOINT"]
if "AWS_ACCESS_KEY_ID" in os.environ:
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
if "AWS_SECRET_ACCESS_KEY" in os.environ:
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

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
    
    # 接続テスト
    s3_client.list_buckets()
    print(f"MinIO接続成功: {endpoint_url}")
except Exception as e:
    print(f"MinIO接続エラー: {e}")
    sys.exit(1)

# コマンド処理
command = sys.argv[1]

if command == "list_buckets":
    # バケット一覧の取得
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
    
    bucket_name = sys.argv[2]
    key = sys.argv[3]
    content = sys.argv[4]
    
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
