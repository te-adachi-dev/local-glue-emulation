# AWS Glue ローカルエミュレータ

このプロジェクトはAWS Glueをローカル環境でエミュレートするためのフレームワークです。Docker上でGlue、Hive Metastore、Trinoを連携させて、AWS Glueと同様の機能をローカルで実現します。

## 環境構成

このエミュレータは以下のコンポーネントで構成されています：

- **Glue コンテナ**: AWS Glueの実行環境をエミュレート
- **Hive Metastore**: データカタログ情報を管理
- **MySQL**: Hive Metastoreのバックエンドデータベース
- **Trino**: 分散SQLクエリエンジン

## AWS対応関係

| AWS サービス | ローカルエミュレータでの対応 |
|-------------|--------------------------|
| **S3** | ローカルファイルシステム (`/home/glue_user/workspace/data`) |
| **Glueデータカタログ** | Hive Metastore |
| **Glueクローラ** | `crawler/` ディレクトリ内のPythonスクリプト |
| **Glueジョブ** | `jobs/` ディレクトリ内のPythonスクリプト |
| **Athena** | Trinoクエリエンジン |

## セットアップ

### 前提条件

- Docker
- Docker Compose
- Git

### インストール

```bash
git clone <リポジトリURL>
cd glue-local-emulator
chmod +x scripts/*.sh
```

## 環境の起動

```bash
docker-compose up -d
```

## データの配置と登録

### 1. データファイルの配置

CSVなどのデータファイルを `data/input/csv/` ディレクトリに配置します：

```bash
mkdir -p data/input/csv
# ここにCSVファイルをコピー
```

### 2. データの登録

クローラを実行してデータをHive Metastoreに登録します：

```bash
docker exec glue20250407 python3 /home/glue_user/workspace/crawler/crawler_container.py
```

### 3. データ登録の確認

登録されたデータをHiveから確認します：

```bash
./scripts/execute_sql.sh "SHOW DATABASES;"
./scripts/execute_sql.sh "USE test_db_20250407; SHOW TABLES;"
./scripts/execute_sql.sh "SELECT * FROM test_db_20250407.<テーブル名> LIMIT 10;"
```

または、Trinoから確認：

```bash
docker exec trino_20250407 trino --server localhost:8080 --catalog hive --schema test_db_20250407 --execute "SELECT * FROM <テーブル名> LIMIT 10"
```

## Glueジョブの実行

サンプルジョブを実行してデータを処理します：

```bash
docker exec glue20250407 python3 /home/glue_user/workspace/jobs/test_job_hive.py --JOB_NAME test_job --database test_db_20250407 --table <テーブル名>
```

処理結果は `data/output/processed_data` に出力されます。

## 権限問題の回避

すべてのファイルとディレクトリに適切な権限を与えるには：

```bash
chmod -R 777 data hive-data
```

Dockerコンテナは `privileged: true` オプションで実行されており、ホストのリソースに完全にアクセスできるように設定されています。

## 使用後の削除

環境が不要になった場合は、以下のコマンドで削除できます：

```bash
docker-compose down -v
rm -rf mysql-data hive-data trino-data temp_ddl.hql
```

## ディレクトリ構造

```
.
├── Dockerfile - Glueコンテナのビルド定義
├── docker-compose.yml - 複数コンテナの定義と連携設定
├── hive-metastore.Dockerfile - Hive Metastoreのビルド定義
├── conf/ - 各種設定ファイル
│   ├── hive-conf/ - Hive設定
│   └── trino-conf/ - Trino設定
├── crawler/ - データクローラスクリプト
│   └── crawler_container.py - メインクローラスクリプト
├── jobs/ - Glueジョブスクリプト
│   └── test_job_hive.py - 基本テストジョブ
└── scripts/ - 各種操作用スクリプト
    └── execute_sql.sh - SQLクエリ実行
```

## トラブルシューティング

1. **権限エラー**: データディレクトリに書き込み権限がない場合は以下を実行：
   ```bash
   chmod -R 777 data hive-data
   ```

2. **コンテナ接続エラー**: 各コンテナが正常に起動しているか確認：
   ```bash
   docker-compose ps
   ```

3. **Hive Metastore接続エラー**:
   ```bash
   docker-compose restart hive-metastore
   ```

4. **MySQLの役割**: MySQLはHive Metastoreのバックエンドデータベースとして必要です。
   テーブルスキーマなどのメタデータが保存されています。
