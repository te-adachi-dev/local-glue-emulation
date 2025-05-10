#!/bin/bash
# CSVファイルをMinIOにアップロードするスクリプト (Pythonバージョン)

set -e

echo "=== CSVファイルをMinIOにアップロードするスクリプト (Pythonバージョン) ==="

# 変数設定
CONTAINER_NAME="glue20250407"
BUCKET_NAME="glue-bucket"
LOCAL_CSV_DIR="./data/input/csv"

# カレントディレクトリ取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SIMPLE_MINIO_CLIENT="${DIR}/simple_minio_client.py"

# 入力ディレクトリを引数から取得
if [ "$1" != "" ]; then
    LOCAL_CSV_DIR="$1"
    echo "カスタム入力ディレクトリ: $LOCAL_CSV_DIR"
fi

# MinIOが起動しているか確認
if ! docker exec $CONTAINER_NAME curl --output /dev/null --silent --head --fail http://minio:9000/minio/health/live; then
    echo "エラー: MinIOサーバーが起動していません"
    exit 1
fi

# CSVファイルのアップロード
echo "CSVファイルをアップロードしています..."

# 入力CSVファイル
if [ -d "${LOCAL_CSV_DIR}" ]; then
    for csv_file in ${LOCAL_CSV_DIR}/*; do
        if [ -f "${csv_file}" ]; then
            filename=$(basename "${csv_file}")
            
            # コンテナに一時ファイルをコピー
            echo "ファイルをコンテナにコピー中: ${filename}"
            docker cp "${csv_file}" "${CONTAINER_NAME}:/tmp/${filename}"
            
            # ファイルの場所に応じたパスを設定
            case "${filename}" in
                "master_device_attribute.csv")
                    s3_path="master/device_attribute/${filename}"
                    ;;
                "master_status.csv")
                    s3_path="master/status/${filename}"
                    ;;
                "master_user_attribute.csv")
                    s3_path="master/user_attribute/${filename}"
                    ;;
                *)
                    s3_path="input/csv/${filename}"
                    ;;
            esac
            
            echo "アップロード中: ${filename} -> s3://${BUCKET_NAME}/${s3_path}"
            docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
                        -e AWS_SECRET_ACCESS_KEY=minioadmin \
                        $CONTAINER_NAME python3 /tmp/simple_minio_client.py copy_file /tmp/${filename} ${BUCKET_NAME} ${s3_path}
            
            # 一時ファイルを削除
            docker exec $CONTAINER_NAME rm /tmp/${filename}
        fi
    done
    echo "入力CSVファイルのアップロードが完了しました"
else
    echo "警告: CSVディレクトリが見つかりません: ${LOCAL_CSV_DIR}"
    exit 1
fi

echo "すべてのファイルのアップロードが完了しました"
echo "S3バケット内のファイル一覧:"
docker exec -e AWS_ACCESS_KEY_ID=minioadmin \
            -e AWS_SECRET_ACCESS_KEY=minioadmin \
            $CONTAINER_NAME python3 /tmp/simple_minio_client.py list_objects ${BUCKET_NAME}
