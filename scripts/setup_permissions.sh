#!/bin/bash
# スクリプトファイルに実行権限を付与

# 新しく作成したディレクトリ
mkdir -p minio-data
chmod -R 755 minio-data

# 新しく作成したスクリプトに実行権限を付与
chmod +x scripts/setup_minio.sh
chmod +x scripts/upload_csvs_to_minio.sh
chmod +x scripts/run_minio_list_ap.sh
chmod +x scripts/run_minio_connection.sh
chmod +x scripts/run_minio_container_job.sh
chmod +x scripts/test_minio_connection.sh
chmod +x scripts/init_minio_env.sh
chmod +x scripts/setup_permissions.sh

# 既存のスクリプトファイルにも確実に実行権限を付与
chmod +x scripts/run_container_job.sh
chmod +x scripts/*.sh

echo "すべてのスクリプトファイルに実行権限を付与しました。"
