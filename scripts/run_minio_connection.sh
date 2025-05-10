#!/bin/bash
# MinIO版connectionジョブ単独実行スクリプト

# 引数のチェック
if [ $# -lt 1 ]; then
    echo "使用方法: $0 <YYYYMM> [JOB_NAME]"
    echo "例: $0 202501 connection_job"
    exit 1
fi

YYYYMM=$1
JOB_NAME=${2:-"connection_job"}

# カレントディレクトリを取得
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JOBS_DIR="$DIR/../jobs"

# PYTHONPATH設定でBUCKET_NAME.pyを確実に読み込めるようにする
export PYTHONPATH=$PYTHONPATH:$JOBS_DIR

# 環境変数の設定
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=ap-northeast-1

echo "=== MinIO版 connection ジョブ実行 ==="
echo "処理対象年月: $YYYYMM"
echo "ジョブ名: $JOB_NAME"

# Pythonスクリプト実行
python3 "$JOBS_DIR/minio_connection_job.py" --YYYYMM="$YYYYMM"

exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "ジョブが正常に完了しました。"
else
    echo "ジョブが失敗しました。(終了コード: $exit_code)"
fi

exit $exit_code
