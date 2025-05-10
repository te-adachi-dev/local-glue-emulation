#!/bin/bash
# AWS Glueã³ã³ããåã§ã¸ã§ããå®è¡ããã¹ã¯ãªãã
set -e

echo "=== AWS Glue ã³ã³ããåã¸ã§ãå®è¡ã¹ã¯ãªãã ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - ã¸ã§ãå®è¡éå§"

# å®è¡ããã¸ã§ããé¸æ
echo "å®è¡ããã¸ã§ããé¸æãã¦ãã ãã:"
echo "1) list_ap ã¸ã§ã"
echo "2) connection ã¸ã§ã"
echo "3) ä¸¡æ¹å®è¡"
read -p "é¸æ (1/2/3): " job_choice

# å¦çå¯¾è±¡å¹´æ
read -p "å¦çå¯¾è±¡å¹´æ (YYYYMMå½¢å¼ãä¾: 202501): " yyyymm

# å¥åãã§ãã¯
if [[ ! $yyyymm =~ ^[0-9]{6}$ ]]; then
    echo "ã¨ã©ã¼: YYYYMMã®å½¢å¼ãæ­£ããããã¾ãããä¾: 202501"
    exit 1
fi

# å®è¡ãã£ã¬ã¯ããªã®è¨­å®
BASE_DIR="$(cd "$(dirname "$0")" && pwd)/.."
JOBS_DIR="${BASE_DIR}/jobs"
DATA_DIR="${BASE_DIR}/data"
OUTPUT_DIR="${DATA_DIR}/output"

# åºåãã£ã¬ã¯ããªã®æºå
mkdir -p "${OUTPUT_DIR}/list_ap_output/list_ap_table"
mkdir -p "${OUTPUT_DIR}/list_ap_output/list_ap_backup"
mkdir -p "${OUTPUT_DIR}/list_ap_output/common_ap"
mkdir -p "${OUTPUT_DIR}/connection_output/yearmonth=${yyyymm}"

# å¥åãã¼ã¿ã®ç¢ºèª
echo "å¥åãã¼ã¿ãç¢ºèªãã¦ãã¾ã..."
CSV_DIR="${DATA_DIR}/input/csv"
required_files=("input_ap.csv" "input_connection.csv" "master_device_attribute.csv" "master_status.csv" "master_user_attribute.csv")

for file in "${required_files[@]}"; do
    if [[ ! -f "${CSV_DIR}/${file}" ]]; then
        echo "ã¨ã©ã¼: å¿è¦ãªãã¡ã¤ã« ${file} ãè¦ã¤ããã¾ããã"
        echo "ã¯ã­ã¼ã©ã¼ãåã«å®è¡ãã¦ãã ããã"
        exit 1
    fi
done

echo "å¿è¦ãªãã¡ã¤ã«ããã¹ã¦ç¢ºèªãã¾ããã"

# ã¸ã§ããã¡ã¤ã«ãã³ã³ããåã«ã³ãã¼
echo "ã¸ã§ããã¡ã¤ã«ãGlueã³ã³ããã«ã³ãã¼ãã¦ãã¾ã..."
CONTAINER_NAME="glue20250407"

# jobs ãã£ã¬ã¯ããªãã³ã³ããåã«ä½æ
docker exec -it $CONTAINER_NAME mkdir -p /home/glue_user/workspace/jobs

# ã¸ã§ãã¹ã¯ãªãããã³ã³ããåã«ã³ãã¼
docker cp ${JOBS_DIR}/list_ap_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/
docker cp ${JOBS_DIR}/connection_job.py $CONTAINER_NAME:/home/glue_user/workspace/jobs/

echo "ã¸ã§ããã¡ã¤ã«ãã³ãã¼ãã¾ããã"

# list_apã¸ã§ãã®å®è¡é¢æ°
run_list_ap_job() {
    echo "list_ap ã¸ã§ããã³ã³ããåã§å®è¡ãã¦ãã¾ã..."
    docker exec -it $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/list_ap_job.py --JOB_NAME="list_ap_job" --YYYYMM="${yyyymm}"
    
    if [[ $? -eq 0 ]]; then
        echo "list_ap ã¸ã§ããæ­£å¸¸ã«å®äºãã¾ããã"
        # åºåãã¡ã¤ã«ã®ãªã¹ãè¡¨ç¤º
        echo "åºåãã¡ã¤ã«:"
        ls -la "${OUTPUT_DIR}/list_ap_output"/*
    else
        echo "ã¨ã©ã¼: list_ap ã¸ã§ãã®å®è¡ã«å¤±æãã¾ããã"
        exit 1
    fi
}

# connectionã¸ã§ãã®å®è¡é¢æ°
run_connection_job() {
    echo "connection ã¸ã§ããã³ã³ããåã§å®è¡ãã¦ãã¾ã..."
    docker exec -it $CONTAINER_NAME python3 /home/glue_user/workspace/jobs/connection_job.py --YYYYMM="${yyyymm}"
    
    if [[ $? -eq 0 ]]; then
        echo "connection ã¸ã§ããæ­£å¸¸ã«å®äºãã¾ããã"
        # åºåãã¡ã¤ã«ã®ãªã¹ãè¡¨ç¤º
        echo "åºåãã¡ã¤ã«:"
        ls -la "${OUTPUT_DIR}/connection_output"/*
    else
        echo "ã¨ã©ã¼: connection ã¸ã§ãã®å®è¡ã«å¤±æãã¾ããã"
        exit 1
    fi
}

# é¸æã«åºã¥ãã¦ã¸ã§ããå®è¡
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
        echo "ç¡å¹ãªé¸æã§ãã1ã2ãã¾ãã¯3ãå¥åãã¦ãã ããã"
        exit 1
        ;;
esac

echo "$(date '+%Y-%m-%d %H:%M:%S') - å¦çãå®äºãã¾ãã"
