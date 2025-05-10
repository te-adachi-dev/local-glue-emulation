#!/bin/bash
# AWS認証情報をDockerコンテナに反映させるスクリプト

set -e

echo "=== AWS認証情報をDockerコンテナに反映 ==="

CONTAINER_NAME="glue20250407"
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

# 環境変数をdocker-compose.ymlにも反映させる
sed -i 's/AWS_ACCESS_KEY_ID=.*/AWS_ACCESS_KEY_ID=minioadmin/' docker-compose.yml
sed -i 's/AWS_SECRET_ACCESS_KEY=.*/AWS_SECRET_ACCESS_KEY=minioadmin/' docker-compose.yml

# コンテナのファイルシステムに直接反映
docker exec -u root $CONTAINER_NAME bash -c "echo 'export AWS_ACCESS_KEY_ID=minioadmin' >> /etc/environment"
docker exec -u root $CONTAINER_NAME bash -c "echo 'export AWS_SECRET_ACCESS_KEY=minioadmin' >> /etc/environment"
docker exec -u root $CONTAINER_NAME bash -c "echo 'export AWS_DEFAULT_REGION=ap-northeast-1' >> /etc/environment"

# .aws/credentialsと.aws/configファイルを作成
docker exec $CONTAINER_NAME bash -c "mkdir -p ~/.aws"
docker exec $CONTAINER_NAME bash -c "cat > ~/.aws/credentials << EOL
[default]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
EOL"

docker exec $CONTAINER_NAME bash -c "cat > ~/.aws/config << EOL
[default]
region = ap-northeast-1
s3 =
    signature_version = s3v4
    addressing_style = path
EOL"

# bashrcにも追加
docker exec $CONTAINER_NAME bash -c "echo 'export AWS_ACCESS_KEY_ID=minioadmin' >> ~/.bashrc"
docker exec $CONTAINER_NAME bash -c "echo 'export AWS_SECRET_ACCESS_KEY=minioadmin' >> ~/.bashrc"
docker exec $CONTAINER_NAME bash -c "echo 'export AWS_DEFAULT_REGION=ap-northeast-1' >> ~/.bashrc"

echo "AWS認証情報を設定しました。コンテナを再起動してください。"
echo "例: docker-compose restart glue-local"
