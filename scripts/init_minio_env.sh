#!/bin/bash
# MinIO環境の一括セットアップスクリプト

set -e

echo "=== MinIO対応Glueエミュレーション環境セットアップ ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - セットアップ開始"

# 現在のディレクトリを取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$DIR/.."

# ディレクトリの作成
mkdir -p ${BASE_DIR}/minio-data
mkdir -p ${BASE_DIR}/data/input/csv
mkdir -p ${BASE_DIR}/data/output/list_ap_output/list_ap_table
mkdir -p ${BASE_DIR}/data/output/list_ap_output/list_ap_backup
mkdir -p ${BASE_DIR}/data/output/list_ap_output/common_ap
mkdir -p ${BASE_DIR}/data/output/connection_output

# BUCKET_NAME.py ファイルの作成（存在しない場合）
BUCKET_NAME_PY=${BASE_DIR}/jobs/BUCKET_NAME.py
if [ ! -f "$BUCKET_NAME_PY" ]; then
    echo "BUCKET_NAME.py ファイルを作成します..."
    cat > "$BUCKET_NAME_PY" << EOL
# 本番環境のバケット名
OPEN = "ap-open-bucket"  # 外部公開データ用バケット
MIDDLE = "ap-middle-bucket"  # 中間データ用バケット
MASTER = "ap-master-bucket"  # マスターデータ用バケット
INITIAL = "ap-initial-bucket"  # 初期データ用バケット

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

# スクリプトファイルに実行権限を付与
echo "スクリプトファイルに実行権限を付与します..."
chmod +x ${BASE_DIR}/scripts/*.sh

# Docker Composeでコンテナを起動
echo "Docker Composeでコンテナを起動します..."
cd ${BASE_DIR}
docker-compose up -d

# MinIOの初期設定
echo "MinIOの初期設定を実行します..."
${BASE_DIR}/scripts/setup_minio.sh local

# CSVファイルのアップロード
echo "CSVファイルをMinIOにアップロードします..."
${BASE_DIR}/scripts/upload_csvs_to_minio.sh local ${BASE_DIR}/data/input/csv

echo "$(date '+%Y-%m-%d %H:%M:%S') - セットアップ完了"
echo ""
echo "MinIO対応のGlueジョブを実行するには以下のコマンドを使用してください："
echo "  - コンテナ内で実行：./scripts/run_minio_container_job.sh"
echo "  - ホスト側で個別実行："
echo "      ./scripts/run_minio_list_ap.sh [YYYYMM]"
echo "      ./scripts/run_minio_connection.sh [YYYYMM]"
echo ""
echo "MinIO管理コンソール："
echo "  URL: http://localhost:9001"
echo "  ユーザー: minioadmin"
echo "  パスワード: minioadmin"
