#!/bin/bash
# CSVファイルをMinIOにアップロードするスクリプト

set -e

echo "=== CSVファイルをMinIOにアップロードするスクリプト ==="

# 変数設定
MINIO_ENDPOINT="http://minio:9000"
BUCKET_NAME="glue-bucket"
LOCAL_CSV_DIR="./data/input/csv"

# ホスト側で実行する場合はlocalhostに変更
if [ "$1" == "local" ]; then
    MINIO_ENDPOINT="http://localhost:9000"
    echo "ローカル実行モード: エンドポイントを $MINIO_ENDPOINT に設定"
fi

# 入力ディレクトリを引数から取得
if [ "$2" != "" ]; then
    LOCAL_CSV_DIR="$2"
    echo "カスタム入力ディレクトリ: $LOCAL_CSV_DIR"
fi

# MinIOが起動しているか確認
if ! curl --output /dev/null --silent --head --fail ${MINIO_ENDPOINT}/minio/health/live; then
    echo "エラー: MinIOサーバーが起動していません"
    exit 1
fi

# AWS CLIの設定
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_DEFAULT_REGION="ap-northeast-1"

# CSVファイルのアップロード
echo "CSVファイルをアップロードしています..."

# 入力CSVファイル
if [ -d "${LOCAL_CSV_DIR}" ]; then
    for csv_file in ${LOCAL_CSV_DIR}/*; do
        if [ -f "${csv_file}" ]; then
            filename=$(basename "${csv_file}")
            
            # ファイルの場所に応じたパスを設定
            case "${filename}" in
                "master_device_attribute.csv")
                    s3_path="s3://${BUCKET_NAME}/master/device_attribute/${filename}"
                    ;;
                "master_status.csv")
                    s3_path="s3://${BUCKET_NAME}/master/status/${filename}"
                    ;;
                "master_user_attribute.csv")
                    s3_path="s3://${BUCKET_NAME}/master/user_attribute/${filename}"
                    ;;
                *)
                    s3_path="s3://${BUCKET_NAME}/input/csv/${filename}"
                    ;;
            esac
            
            echo "アップロード中: ${filename} -> ${s3_path}"
            aws --endpoint-url=${MINIO_ENDPOINT} s3 cp "${csv_file}" "${s3_path}"
        fi
    done
    echo "入力CSVファイルのアップロードが完了しました"
else
    echo "警告: CSVディレクトリが見つかりません: ${LOCAL_CSV_DIR}"
    exit 1
fi

echo "すべてのファイルのアップロードが完了しました"
echo "S3バケット内のファイル一覧:"
aws --endpoint-url=${MINIO_ENDPOINT} s3 ls --recursive s3://${BUCKET_NAME}/
