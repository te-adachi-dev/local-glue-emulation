#!/bin/bash

echo "=== AWS Glue クローラー実行スクリプト ==="
echo "$(date '+%Y-%m-%d %H:%M:%S') - クローラー実行開始"

# エラーが発生したら即終了
set -e

# 1. ディレクトリの確認
if [ ! -d "./data/input/csv" ]; then
    echo "エラー: ./data/input/csv ディレクトリが存在しません"
    echo "setup.sh を実行してディレクトリ構造を準備してください"
    exit 1
fi

# 2. CSVファイルの存在確認
if ! ls ./data/input/csv/*.csv 1> /dev/null 2>&1; then
    echo "警告: ./data/input/csv に CSV ファイルが見つかりません"
    echo "処理を続行しますが、テーブルが作成されない可能性があります"
    read -p "続行しますか？ (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "処理を中止します"
        exit 1
    fi
else
    echo "検出された CSV ファイル:"
    ls -la ./data/input/csv/*.csv
fi

# 3. コンテナ状態の確認
echo "コンテナの状態を確認中..."
if ! docker ps | grep -q "glue20250407"; then
    echo "エラー: glue コンテナが実行されていません"
    echo "docker-compose up -d を実行してください"
    exit 1
fi

if ! docker ps | grep -q "hive_metastore_20250407"; then
    echo "エラー: hive_metastore コンテナが実行されていません"
    echo "docker-compose up -d を実行してください"
    exit 1
fi

# 4. クローラーの実行
echo "クローラーをコンテナ内で実行中..."
if ! docker exec glue20250407 python3 /home/glue_user/workspace/crawler/crawler_container.py; then
    echo "エラー: クローラーの実行に失敗しました"
    exit 1
fi

# 5. DDLファイルの存在確認
echo "DDL ファイルを確認中..."
if docker exec glue20250407 test -f /home/glue_user/temp_ddl.hql; then
    echo "DDL ファイルが生成されました"
    docker cp glue20250407:/home/glue_user/temp_ddl.hql ./crawler/temp_ddl.hql
    echo "DDL ファイルをホストにコピーしました: ./crawler/temp_ddl.hql"
else
    echo "警告: DDL ファイルが生成されませんでした"
    echo "クローラーのログを確認してください"
    docker logs glue20250407
    exit 1
fi

# 6. DDLをHiveメタストアで実行
echo "テーブル作成 DDL を実行中..."
docker cp ./crawler/temp_ddl.hql hive_metastore_20250407:/opt/hive/temp_ddl.hql
if ! docker exec hive_metastore_20250407 /opt/hive/bin/hive -f /opt/hive/temp_ddl.hql; then
    echo "エラー: DDL の実行に失敗しました"
    echo "DDL ファイルを確認してください: ./crawler/temp_ddl.hql"
    exit 1
fi

# 7. 作成されたテーブル一覧の表示
echo "=== 作成されたテーブル一覧 ==="
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SHOW DATABASES; USE test_db_20250407; SHOW TABLES;"

echo "$(date '+%Y-%m-%d %H:%M:%S') - クローラー実行完了"
echo ""
echo "次のステップ:"
echo "Glue ジョブを実行して、登録されたテーブルを処理してください"
echo "例: docker exec glue20250407 python3 /home/glue_user/workspace/jobs/test_job_hive.py --database test_db_20250407 --table <テーブル名>"
