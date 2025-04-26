#!/bin/bash
# AWS Glueコンテナ内でジョブを実行するスクリプト
set -e

echo "=== AWS Glue コンテナ内ジョブ実行スクリプト ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - ジョブ実行開始"

# 実行するジョブを選択
echo "実行するジョブを選択してください:"
echo "1) list_ap ジョブ"
echo "2) connection ジョブ"
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

# 入力データの確認
echo "入力データを確認しています..."
CSV_DIR="${DATA_DIR}/input/csv"
required_files=("input_ap.csv" "input_connection.csv" "master_device_attribute.csv" "master_status.csv" "master_user_attribute.csv")

for file in "${required_files[@]}"; do
    if [[ ! -f "${CSV_DIR}/${file}" ]]; then
        echo "エラー: 必要なファイル ${file} が見つかりません。"
        echo "クローラーを先に実行してください。"
        exit 1
    fi
done

echo "必要なファイルをすべて確認しました。"

# ジョブファイルをコンテナ内にコピー
echo "ジョブファイルをGlueコンテナにコピーしています..."
CONTAINER_NAME="glue20250407"

# jobs ディレクトリをコンテナ内に作成
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/workspace/jobs
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/data/input/csv
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/data/output/list_ap_output
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/data/output/connection_output/yearmonth=${yyyymm}
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/crawler/column_mappings

# ジョブスクリプトをコンテナ内にコピー
docker cp ${JOBS_DIR}/list_ap_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/
docker cp ${JOBS_DIR}/connection_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/

# データファイルをコンテナ内にコピー
echo "データファイルをコンテナにコピーしています..."
for file in "${required_files[@]}"; do
    docker cp ${CSV_DIR}/${file} $CONTAINER_NAME:/home/glue_user/data/input/csv/
done

# カラムマッピングをコンテナ内にコピー
if [ -d "${CRAWLER_DIR}/column_mappings" ]; then
    echo "カラムマッピングファイルをコピーしています..."
    for mapping_file in "${CRAWLER_DIR}"/column_mappings/*; do
        if [ -f "$mapping_file" ]; then
            docker cp "$mapping_file" $CONTAINER_NAME:/home/glue_user/crawler/column_mappings/
        fi
    done
fi

echo "ファイルのコピーが完了しました。"

# コンテナ内での書き込み権限を付与
docker exec -it $CONTAINER_NAME chmod -R 777 /home/glue_user/data 2>/dev/null || true
docker exec -it $CONTAINER_NAME chmod -R 777 /home/glue_user/crawler 2>/dev/null || true




echo "権限設定が完了しました。"

# list_apジョブの実行関数
run_list_ap_job() {
    echo "list_ap ジョブをコンテナ内で実行しています..."
    docker exec -it $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/list_ap_job.py --JOB_NAME="list_ap_job" --YYYYMM="${yyyymm}"
    
    if [[ $? -eq 0 ]]; then
        echo "list_ap ジョブが正常に完了しました。"
        
        # 結果ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."
        docker exec -it $CONTAINER_NAME find /home/glue_user/data/output/list_ap_output -type f -name "*.csv" | while read file; do
            container_dir=$(dirname "$file")
            filename=$(basename "$file")
            host_dir="${OUTPUT_DIR}/list_ap_output/$(basename "$container_dir")"
            mkdir -p "$host_dir"
            docker cp "$CONTAINER_NAME:$file" "$host_dir/$filename"
            echo "コピーしました: $host_dir/$filename"
        done
        
        # 出力ファイルのリスト表示
        echo "出力ファイル:"
        find "${OUTPUT_DIR}/list_ap_output" -type f -name "*.csv" | sort
    else
        echo "エラー: list_ap ジョブの実行に失敗しました。"
        exit 1
    fi
}

# connectionジョブの実行関数
run_connection_job() {
    echo "connection ジョブをコンテナ内で実行しています..."
    docker exec -it $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/connection_job.py --YYYYMM="${yyyymm}"
    
    if [[ $? -eq 0 ]]; then
        echo "connection ジョブが正常に完了しました。"
        
        # 結果ファイルをホストにコピー
        echo "結果ファイルをホストにコピーしています..."
        docker exec -it $CONTAINER_NAME find /home/glue_user/data/output/connection_output -type f -name "*.csv" | while read file; do
            container_dir=$(dirname "$file")
            filename=$(basename "$file")
            rel_path="${container_dir#/home/glue_user/data/output/}"
            host_dir="${OUTPUT_DIR}/${rel_path}"
            mkdir -p "$host_dir"
            docker cp "$CONTAINER_NAME:$file" "$host_dir/$filename"
            echo "コピーしました: $host_dir/$filename"
        done
        
        # 出力ファイルのリスト表示
        echo "出力ファイル:"
        find "${OUTPUT_DIR}/connection_output" -type f -name "*.csv" | sort
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
