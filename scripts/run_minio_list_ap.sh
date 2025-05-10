#!/bin/bash
# MinIO版list_apジョブ単独実行スクリプト

# 引数のチェック
if [ $# -lt 1 ]; then
    echo "使用方法: $0 <YYYYMM> [JOB_NAME]"
    echo "例: $0 202501 list_ap_job"
    exit 1
fi

YYYYMM=$1
JOB_NAME=${2:-"list_ap_job"}

# カレントディレクトリを取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JOBS_DIR="$DIR/../jobs"

# PYTHONPATH設定でBUCKET_NAME.pyとawsglue_mock.pyを確実に読み込めるようにする
export PYTHONPATH=$PYTHONPATH:$JOBS_DIR

# MinIO設定
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=ap-northeast-1

echo "=== MinIO版 list_ap ジョブ実行 ==="
echo "処理対象年月: $YYYYMM"
echo "ジョブ名: $JOB_NAME"

# 必要なモジュールがインストールされているか確認
REQUIRED_MODULES=("boto3" "pyspark")
MISSING_MODULES=()

for module in "${REQUIRED_MODULES[@]}"; do
    if ! python -c "import $module" 2>/dev/null; then
        MISSING_MODULES+=("$module")
    fi
done

if [ ${#MISSING_MODULES[@]} -gt 0 ]; then
    echo "以下の必要なPythonモジュールがインストールされていません:"
    for module in "${MISSING_MODULES[@]}"; do
        echo "  - $module"
    done
    echo "pip install [モジュール名] でインストールしてください。"
    exit 1
fi

# スクリプト実行時に自動的にawsglue_mockをインポートするよう環境変数を設定
export PYTHONPATH=$PYTHONPATH:$JOBS_DIR

# Pythonスクリプト実行
python3 -c "import sys; sys.path.insert(0, '$JOBS_DIR'); import awsglue_mock; from minio_list_ap_job import *" -- --JOB_NAME="$JOB_NAME" --YYYYMM="$YYYYMM"

exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "ジョブが正常に完了しました。"
else
    echo "ジョブが失敗しました。(終了コード: $exit_code)"
fi

exit $exit_code