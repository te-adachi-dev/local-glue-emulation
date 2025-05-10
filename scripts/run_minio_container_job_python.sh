#!/bin/bash
# AWS Glueコンテナ内でMinIO対応ジョブを実行するスクリプト (Python boto3バージョン)
set -e

echo "=== AWS Glue コンテナ内MinIO対応ジョブ実行スクリプト (Python boto3バージョン) ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - ジョブ実行開始"

# 変数設定
CONTAINER_NAME="glue20250407"
BUCKET_NAME="glue-bucket"

# 実行するジョブを選択
echo "実行するジョブを選択してください:"
echo "1) MinIO対応 list_ap ジョブ"
echo "2) MinIO対応 connection ジョブ"
echo "3) 両方実行"
read -p "選択 (1/2/3): " job_choice

# 処理対象年月
read -p "処理対象年月 (YYYYMM形式、例: 202501): " yyyymm

# 入力チェック
if [[ ! $yyyymm =~ ^[0-9]{6}$ ]]; then
    echo "エラー: YYYYMMの形式が正しくありません。例: 202501"
    exit 1
fi

# 実行ディレクトリの設定
BASE_DIR="$(cd "$(dirname "$0")" && pwd)/.."
JOBS_DIR="${BASE_DIR}/jobs"
DATA_DIR="${BASE_DIR}/data"
OUTPUT_DIR="${DATA_DIR}/output"
CRAWLER_DIR="${BASE_DIR}/crawler"

# 出力ディレクトリの準備
mkdir -p "${OUTPUT_DIR}/list_ap_output/list_ap_table"
mkdir -p "${OUTPUT_DIR}/list_ap_output/list_ap_backup"
mkdir -p "${OUTPUT_DIR}/list_ap_output/common_ap"
mkdir -p "${OUTPUT_DIR}/connection_output/yearmonth=${yyyymm}"

# MinIOが利用可能か確認
echo "MinIOサーバーの接続を確認しています..."
if ! docker exec $CONTAINER_NAME curl --output /dev/null --silent --head --fail http://minio:9000/minio/health/live; then
    echo "エラー: MinIOサーバーに接続できません。"
    exit 1
fi

# ジョブファイルをコンテナ内にコピー
echo "ジョブファイルとBUCKET_NAME.pyをGlueコンテナにコピーしています..."

# jobs ディレクトリをコンテナ内に作成
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/workspace/jobs
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/data/input/csv
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/data/output/list_ap_output/list_ap_backup
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/data/output/list_ap_output/list_ap_table
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/data/output/list_ap_output/common_ap
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/data/output/connection_output/yearmonth=${yyyymm}
docker exec $CONTAINER_NAME mkdir -p /home/glue_user/crawler/column_mappings

# BUCKET_NAME.pyファイルを作成
docker exec -u root $CONTAINER_NAME bash -c "cat > /home/glue_user/workspace/jobs/BUCKET_NAME.py << EOL
# 本番環境のバケット名
OPEN = \"ap-open-bucket\"  # 外部公開データ用バケット
MIDDLE = \"ap-middle-bucket\"  # 中間データ用バケット
MASTER = \"ap-master-bucket\"  # マスターデータ用バケット
INITIAL = \"ap-initial-bucket\"  # 初期データ用バケット

# ローカル環境のバケット名（MinIO）
LOCAL_BUCKET = \"glue-bucket\"

# マスターデータのパス（本番環境）
MASTER_DEVICE_PATH = \"device_attribute/\"
MASTER_STATUS_PATH = \"status/\"
MASTER_USER_PATH = \"user_attribute/\"

# 入力データのパス（本番環境）
INITIAL_BASE_PATH = \"input/input_connection/\"

# 出力データのパス（本番環境）
MIDDLE_BASE_PATH = \"common/common_connection/\"
EOL"
docker exec -u root $CONTAINER_NAME chmod 666 /home/glue_user/workspace/jobs/BUCKET_NAME.py

# カレントディレクトリ取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SIMPLE_MINIO_CLIENT="${DIR}/simple_minio_client.py"

# MinIOクライアントスクリプトをコンテナにコピー
echo "MinIOクライアントスクリプトをコンテナにコピー..."
docker cp "${SIMPLE_MINIO_CLIENT}" "${CONTAINER_NAME}:/tmp/simple_minio_client.py"
docker exec -u root $CONTAINER_NAME chmod +x /tmp/simple_minio_client.py

# ジョブスクリプトをコンテナ内にコピー
docker cp ${JOBS_DIR}/minio_list_ap_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/
docker cp ${JOBS_DIR}/minio_connection_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/
docker cp ${JOBS_DIR}/BUCKET_NAME.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/

# コンテナ内での書き込み権限を付与
docker exec -u root $CONTAINER_NAME chmod -R 777 /home/glue_user/data 2>/dev/null || true
docker exec -u root $CONTAINER_NAME chmod -R 777 /home/glue_user/crawler 2>/dev/null || true
docker exec -u root $CONTAINER_NAME chmod -R 777 /home/glue_user/workspace 2>/dev/null || true

echo "権限設定が完了しました。"

# list_apジョブの実行関数
run_list_ap_job() {
    echo "MinIO版 list_ap ジョブをコンテナ内で実行しています..."
    # 環境変数を設定してジョブを実行
    docker exec -it -e AWS_ACCESS_KEY_ID=minioadmin \
                    -e AWS_SECRET_ACCESS_KEY=minioadmin \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    -e MINIO_ENDPOINT=http://minio:9000 \
                    $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/minio_list_ap_job.py --JOB_NAME="list_ap_job" --YYYYMM="${yyyymm}"

    if [[ $? -eq 0 ]]; then
        echo "list_ap ジョブが正常に完了しました。"

        # コンテナ内の出力ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."
        
        # バックアップファイルのコピー
        BACKUP_FILE="/home/glue_user/data/output/list_ap_output/list_ap_backup/backup_listap${yyyymm}.csv"
        if docker exec $CONTAINER_NAME test -f "$BACKUP_FILE"; then
            docker cp "$CONTAINER_NAME:$BACKUP_FILE" "${OUTPUT_DIR}/list_ap_output/list_ap_backup/"
            echo "コピー完了: $(basename "$BACKUP_FILE")"
        else
            echo "警告: バックアップファイルが見つかりません: $BACKUP_FILE"
        fi
        
        # テーブルファイルのコピー
        TABLE_FILE="/home/glue_user/data/output/list_ap_output/list_ap_table/listap${yyyymm}.csv"
        if docker exec $CONTAINER_NAME test -f "$TABLE_FILE"; then
            docker cp "$CONTAINER_NAME:$TABLE_FILE" "${OUTPUT_DIR}/list_ap_output/list_ap_table/"
            echo "コピー完了: $(basename "$TABLE_FILE")"
        else
            echo "警告: テーブルファイルが見つかりません: $TABLE_FILE"
        fi
        
        # 共通APファイルのコピー（パーティション付き）
        COMMON_DIR="/home/glue_user/data/output/list_ap_output/common_ap"
        docker exec $CONTAINER_NAME bash -c "find '$COMMON_DIR' -type f -name '*.csv'" | while read file; do
            if [ -n "$file" ]; then
                # パーティションパスを維持しつつコピー
                relative_path=$(echo "$file" | sed "s|$COMMON_DIR/||")
                target_dir="${OUTPUT_DIR}/list_ap_output/common_ap/$(dirname "$relative_path")"
                mkdir -p "$target_dir"
                docker cp "$CONTAINER_NAME:$file" "$target_dir/"
                echo "コピー完了: $relative_path"
            fi
        done

        # 出力ファイルのリスト表示
        echo "ホスト上の出力ファイル:"
        find "${OUTPUT_DIR}/list_ap_output" -type f | sort
    else
        echo "エラー: list_ap ジョブの実行に失敗しました。"
        exit 1
    fi
}

# connectionジョブの実行関数
run_connection_job() {
    echo "MinIO版 connection ジョブをコンテナ内で実行しています..."
    # 環境変数を設定してジョブを実行
    docker exec -it -e AWS_ACCESS_KEY_ID=minioadmin \
                    -e AWS_SECRET_ACCESS_KEY=minioadmin \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    -e MINIO_ENDPOINT=http://minio:9000 \
                    $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/minio_connection_job.py --YYYYMM="${yyyymm}"

    if [[ $? -eq 0 ]]; then
        echo "connection ジョブが正常に完了しました。"

        # コンテナ内の出力ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."
        
        # 特定のディレクトリからファイルをコピー
        SOURCE_DIR="/home/glue_user/data/output/connection_output/yearmonth=${yyyymm}"
        if docker exec $CONTAINER_NAME test -d "$SOURCE_DIR"; then
            docker exec $CONTAINER_NAME bash -c "find '$SOURCE_DIR' -type f -name '*.csv'" | while read file; do
                if [ -n "$file" ]; then
                    target_dir="${OUTPUT_DIR}/connection_output/yearmonth=${yyyymm}"
                    mkdir -p "$target_dir"
                    docker cp "$CONTAINER_NAME:$file" "$target_dir/"
                    echo "コピー完了: $(basename "$file")"
                fi
            done
        else
            echo "警告: 出力ディレクトリが見つかりません: $SOURCE_DIR"
        fi

        # 出力ファイルのリスト表示
        echo "ホスト上の出力ファイル:"
        find "${OUTPUT_DIR}/connection_output" -type f | sort
    else
        echo "エラー: connection ジョブの実行に失敗しました。"
        exit 1
    fi
}

# 選択に基づいてジョブを実行
case $job_choice in
    1)
        run_list_ap_job
        ;;
    2)
        run_connection_job
        ;;
    3)
        run_list_ap_job
        run_connection_job
        ;;
    *)
        echo "無効な選択です。1、2、または3を入力してください。"
        exit 1
        ;;
esac

echo "$(date '+%Y-%m-%d %H:%M:%S') - 処理が完了しました"
