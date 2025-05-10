#!/bin/bash
# AWS Glueコンテナ内でMinIO対応ジョブを実行するスクリプト
set -e

echo "=== AWS Glue コンテナ内MinIO対応ジョブ実行スクリプト ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - ジョブ実行開始"

# MinIO設定
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
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

# AWS CLIの設定（コンテナ内用）
echo "AWS CLI設定をコンテナに適用しています..."
CONTAINER_NAME="glue20250407"

# MinIOの接続情報をコンテナに設定（環境変数として直接設定）
docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
            -e AWS_DEFAULT_REGION=ap-northeast-1 \
            "${CONTAINER_NAME}" bash -c "\
                aws configure set aws_access_key_id ${MINIO_ACCESS_KEY} && \
                aws configure set aws_secret_access_key ${MINIO_SECRET_KEY} && \
                aws configure set default.region ap-northeast-1 && \
                aws configure set default.s3.signature_version s3v4 && \
                aws configure set default.s3.addressing_style path"

# MinIOが利用可能か確認
echo "MinIOサーバーの接続を確認しています..."
if ! docker exec "${CONTAINER_NAME}" curl --output /dev/null --silent --head --fail "${MINIO_ENDPOINT}/minio/health/live"; then
    echo "エラー: MinIOサーバーに接続できません。"
    exit 1
fi

# S3バケットの確認
echo "S3バケット確認中..."
if ! docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                -e AWS_DEFAULT_REGION=ap-northeast-1 \
                "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 ls "s3://${BUCKET_NAME}" &>/dev/null; then
    echo "バケット '${BUCKET_NAME}' が見つかりません。作成します..."
    docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                -e AWS_DEFAULT_REGION=ap-northeast-1 \
                "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 mb "s3://${BUCKET_NAME}"

    # 基本的なディレクトリ構造を作成
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
        docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                    -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3api put-object \
                    --bucket "${BUCKET_NAME}" --key "${path}" --content-length 0
    done
    echo "バケットとディレクトリ構造を作成しました。"
fi

# 入力データの確認
echo "S3/MinIO上の入力データを確認しています..."
required_files=("input_ap.csv" "input_connection.csv" "master_device_attribute.csv" "master_status.csv" "master_user_attribute.csv")
missing_files=()

# 必要なファイルの存在確認
for file in "${required_files[@]}"; do
    case "${file}" in
        "master_device_attribute.csv")
            s3_path="s3://${BUCKET_NAME}/master/device_attribute/${file}"
            ;;
        "master_status.csv")
            s3_path="s3://${BUCKET_NAME}/master/status/${file}"
            ;;
        "master_user_attribute.csv")
            s3_path="s3://${BUCKET_NAME}/master/user_attribute/${file}"
            ;;
        *)
            s3_path="s3://${BUCKET_NAME}/input/csv/${file}"
            ;;
    esac

    if ! docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                    -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 ls "${s3_path}" &>/dev/null; then
        missing_files+=("${file}")
    fi
done

if [ ${#missing_files[@]} -gt 0 ]; then
    echo "S3/MinIO上に以下の必要なファイルがありません。アップロードを試みます:"
    for missing in "${missing_files[@]}"; do
        echo "  - ${missing}"
    done

    # ローカルCSVファイルの確認とアップロード
    CSV_DIR="${DATA_DIR}/input/csv"
    if [ -d "${CSV_DIR}" ]; then
        for file in "${missing_files[@]}"; do
            local_file="${CSV_DIR}/${file}"
            if [ -f "${local_file}" ]; then
                echo "ファイルを見つけました: ${local_file}"
                case "${file}" in
                    "master_device_attribute.csv")
                        s3_path="s3://${BUCKET_NAME}/master/device_attribute/${file}"
                        ;;
                    "master_status.csv")
                        s3_path="s3://${BUCKET_NAME}/master/status/${file}"
                        ;;
                    "master_user_attribute.csv")
                        s3_path="s3://${BUCKET_NAME}/master/user_attribute/${file}"
                        ;;
                    *)
                        s3_path="s3://${BUCKET_NAME}/input/csv/${file}"
                        ;;
                esac

                echo "S3/MinIOにアップロード中: ${local_file} -> ${s3_path}"
                docker cp "${local_file}" "${CONTAINER_NAME}:/tmp/${file}"
                docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                            -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                            -e AWS_DEFAULT_REGION=ap-northeast-1 \
                            "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 cp "/tmp/${file}" "${s3_path}"
                docker exec "${CONTAINER_NAME}" rm "/tmp/${file}"
            else
                echo "警告: ファイルが見つかりません: ${local_file}"
            fi
        done
    else
        echo "警告: CSVディレクトリが見つかりません: ${CSV_DIR}"
    fi

    # 再確認
    missing_files=()
    for file in "${required_files[@]}"; do
        case "${file}" in
            "master_device_attribute.csv")
                s3_path="s3://${BUCKET_NAME}/master/device_attribute/${file}"
                ;;
            "master_status.csv")
                s3_path="s3://${BUCKET_NAME}/master/status/${file}"
                ;;
            "master_user_attribute.csv")
                s3_path="s3://${BUCKET_NAME}/master/user_attribute/${file}"
                ;;
            *)
                s3_path="s3://${BUCKET_NAME}/input/csv/${file}"
                ;;
        esac

        if ! docker exec -e AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY} \
                        -e AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY} \
                        -e AWS_DEFAULT_REGION=ap-northeast-1 \
                        "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 ls "${s3_path}" &>/dev/null; then
            missing_files+=("${file}")
        fi
    done

    if [ ${#missing_files[@]} -gt 0 ]; then
        echo "エラー: 以下の必要なファイルがS3/MinIOにありません:"
        for missing in "${missing_files[@]}"; do
            echo "  - ${missing}"
        done
        echo "クローラーを先に実行するか、ファイルをアップロードしてください。"
        exit 1
    fi
fi

echo "必要なファイルをすべて確認しました。"

# ジョブファイルをコンテナ内にコピー
echo "ジョブファイルとBUCKET_NAME.pyをGlueコンテナにコピーしています..."

# jobs ディレクトリをコンテナ内に作成
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/workspace/jobs
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/data/input/csv
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/data/output/list_ap_output/list_ap_backup
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/data/output/list_ap_output/list_ap_table
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/data/output/list_ap_output/common_ap
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/data/output/connection_output/yearmonth="${yyyymm}"
docker exec "${CONTAINER_NAME}" mkdir -p /home/glue_user/crawler/column_mappings

# BUCKET_NAME.pyファイルを作成
docker exec "${CONTAINER_NAME}" bash -c "cat > /home/glue_user/workspace/jobs/BUCKET_NAME.py << EOL
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

# ジョブスクリプトをコンテナ内にコピー
docker cp "${JOBS_DIR}/minio_list_ap_job.py" "${CONTAINER_NAME}:/home/glue_user/workspace/jobs/"
docker cp "${JOBS_DIR}/minio_connection_job.py" "${CONTAINER_NAME}:/home/glue_user/workspace/jobs/"
docker cp "${JOBS_DIR}/BUCKET_NAME.py" "${CONTAINER_NAME}:/home/glue_user/workspace/jobs/"

# pysparkおよびboto3が利用可能か確認
if ! docker exec "${CONTAINER_NAME}" pip3 list | grep -q pyspark; then
    echo "Pythonモジュールをインストールしています..."
    docker exec "${CONTAINER_NAME}" pip3 install pyspark boto3
fi

echo "ファイルのコピーが完了しました。"

# コンテナ内での書き込み権限を付与
docker exec "${CONTAINER_NAME}" chmod -R 777 /home/glue_user/data 2>/dev/null || true
docker exec "${CONTAINER_NAME}" chmod -R 777 /home/glue_user/crawler 2>/dev/null || true
docker exec "${CONTAINER_NAME}" chmod -R 777 /home/glue_user/workspace 2>/dev/null || true

echo "権限設定が完了しました。"

# list_apジョブの実行関数
run_list_ap_job() {
    echo "MinIO版 list_ap ジョブをコンテナ内で実行しています..."
    docker exec -it -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}" \
                    -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}" \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" python3 /home/glue_user/workspace/jobs/minio_list_ap_job.py \
                    --JOB_NAME="list_ap_job" --YYYYMM="${yyyymm}"

    if [[ $? -eq 0 ]]; then
        echo "list_ap ジョブが正常に完了しました。"

        # コンテナ内の出力ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."

        # バックアップファイルのコピー
        BACKUP_FILE="/home/glue_user/data/output/list_ap_output/list_ap_backup/backup_listap${yyyymm}.csv"
        if docker exec "${CONTAINER_NAME}" test -f "${BACKUP_FILE}"; then
            docker cp "${CONTAINER_NAME}:${BACKUP_FILE}" "${OUTPUT_DIR}/list_ap_output/list_ap_backup/"
            echo "コピー完了: $(basename "${BACKUP_FILE}")"
        else
            echo "警告: バックアップファイルが見つかりません: ${BACKUP_FILE}"
        fi

        # テーブルファイルのコピー
        TABLE_FILE="/home/glue_user/data/output/list_ap_output/list_ap_table/listap${yyyymm}.csv"
        if docker exec "${CONTAINER_NAME}" test -f "${TABLE_FILE}"; then
            docker cp "${CONTAINER_NAME}:${TABLE_FILE}" "${OUTPUT_DIR}/list_ap_output/list_ap_table/"
            echo "コピー完了: $(basename "${TABLE_FILE}")"
        else
            echo "警告: テーブルファイルが見つかりません: ${TABLE_FILE}"
        fi

        # 共通APファイルのコピー（パーティション付き）
        COMMON_DIR="/home/glue_user/data/output/list_ap_output/common_ap"
        docker exec "${CONTAINER_NAME}" bash -c "find '${COMMON_DIR}' -type f -name '*.csv'" | while read -r file; do
            if [ -n "${file}" ]; then
                relative_path=$(echo "${file}" | sed "s|${COMMON_DIR}/||")
                target_dir="${OUTPUT_DIR}/list_ap_output/common_ap/$(dirname "${relative_path}")"
                mkdir -p "${target_dir}"
                docker cp "${CONTAINER_NAME}:${file}" "${target_dir}/"
                echo "コピー完了: ${relative_path}"
            fi
        done

        # MinIOから取得したファイルを表示
        echo "S3/MinIOのファイル一覧を表示します..."
        docker exec -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}" \
                    -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}" \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 ls --recursive "s3://${BUCKET_NAME}/output/list_ap_output/"

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
    docker exec -it -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}" \
                    -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}" \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" python3 /home/glue_user/workspace/jobs/minio_connection_job.py \
                    --YYYYMM="${yyyymm}"

    if [[ $? -eq 0 ]]; then
        echo "connection ジョブが正常に完了しました。"

        # コンテナ内の出力ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."

        # 特定のディレクトリからファイルをコピー
        SOURCE_DIR="/home/glue_user/data/output/connection_output/yearmonth=${yyyymm}"
        if docker exec "${CONTAINER_NAME}" test -d "${SOURCE_DIR}"; then
            docker exec "${CONTAINER_NAME}" bash -c "find '${SOURCE_DIR}' -type f -name '*.csv'" | while read -r file; do
                if [ -n "${file}" ]; then
                    target_dir="${OUTPUT_DIR}/connection_output/yearmonth=${yyyymm}"
                    mkdir -p "${target_dir}"
                    docker cp "${CONTAINER_NAME}:${file}" "${target_dir}/"
                    echo "コピー完了: $(basename "${file}")"
                fi
            done
        else
            echo "警告: 出力ディレクトリが見つかりません: ${SOURCE_DIR}"
        fi

        # MinIOから取得したファイルを表示
        echo "S3/MinIOのファイル一覧を表示します..."
        docker exec -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}" \
                    -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}" \
                    -e AWS_DEFAULT_REGION=ap-northeast-1 \
                    "${CONTAINER_NAME}" aws --endpoint-url="${MINIO_ENDPOINT}" s3 ls --recursive "s3://${BUCKET_NAME}/output/connection_output/"

        # 出力ファイルのリスト表示
        echo "ホスト上の出力ファイル:"
        find "${OUTPUT_DIR}/connection_output" -type f | sort
    else
        echo "エラー: connection ジョブの実行に失敗しました。"
        exit 1
    fi
}

# 選択に基づいてジョブを実行
case "${job_choice}" in
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
