# AWS Glue ローカルエミュレータ（MinIO S3対応版）

このプロジェクトはAWS Glueをローカル環境でエミュレートするためのフレームワークです。Docker上でGlue、Hive Metastore、Trino、MinIOを連携させて、AWS Glueと同様の機能をローカルで実現します。

## 特徴

- **従来のローカルファイルシステム対応**: 標準的なGlueジョブのエミュレーション
- **MinIO S3対応**: S3互換ストレージを使用したエミュレーション
- **Hive Metastore連携**: データカタログ管理
- **Trino対応**: 分散SQLクエリエンジン
- **Jupyter Lab統合**: インタラクティブな開発環境

## 環境構成

このエミュレータは以下のコンポーネントで構成されています：

- **Glue コンテナ**: AWS Glueの実行環境をエミュレート
- **Hive Metastore**: データカタログ情報を管理
- **MySQL**: Hive Metastoreのバックエンドデータベース
- **Trino**: 分散SQLクエリエンジン
- **MinIO**: S3互換オブジェクトストレージ

## AWS対応関係

| AWS サービス | 従来版エミュレータ | MinIO対応版エミュレータ |
|------------|------------------|----------------------|
| S3 | ローカルファイルシステム (`/home/glue_user/workspace/data`) | MinIO S3互換ストレージ |
| Glueデータカタログ | Hive Metastore | Hive Metastore |
| Glueクローラ | `crawler/` ディレクトリ内のPythonスクリプト | `crawler/` ディレクトリ内のPythonスクリプト |
| Glueジョブ | `jobs/` ディレクトリ内のPythonスクリプト | `jobs/minio_*.py` スクリプト |
| Athena | Trinoクエリエンジン | Trinoクエリエンジン |

## 1. セットアップ

### 前提条件

- Docker
- Docker Compose
- Git

### インストール

```bash
git clone https://github.com/te-adachi-dev/local-glue-emulation.git
cd local-glue-emulation
```

### 環境の構築と起動

#### 従来版（ローカルファイルシステム使用）

```bash
# セットアップスクリプトを実行
chmod +x setup.sh
./setup.sh
```

#### MinIO S3対応版

```bash
# セットアップスクリプトを実行
chmod +x scripts/init_minio_python.sh
./scripts/init_minio_python.sh
```

起動には数分かかる場合があります。以下のコマンドでコンテナの状態を確認できます：

```bash
docker-compose ps
```

### ディレクトリ構造の説明

```
.
├── Dockerfile - Glueコンテナのビルド定義
├── docker-compose.yml - 複数コンテナの定義と連携設定
├── hive-metastore.Dockerfile - Hive Metastoreのビルド定義
├── conf/ - 各種設定ファイル
├── crawler/ - データクローラスクリプト
├── data/ - 入出力データディレクトリ
├── jobs/ - Glueジョブスクリプト
│   ├── list_ap_job.py - 従来版APリスト処理ジョブ
│   ├── connection_job.py - 従来版接続データ処理ジョブ
│   ├── minio_list_ap_job.py - MinIO対応APリスト処理ジョブ
│   ├── minio_connection_job.py - MinIO対応接続データ処理ジョブ
│   └── BUCKET_NAME.py - バケット名管理モジュール
├── minio-data/ - MinIOデータディレクトリ
├── scripts/ - 各種操作用スクリプト
│   ├── run_crawler.sh - クローラー実行スクリプト
│   ├── run_container_job.sh - 従来版Glueジョブ実行スクリプト
│   ├── run_minio_container_job_python.sh - MinIO対応Glueジョブ実行スクリプト
│   ├── simple_minio_client.py - MinIO操作用Pythonスクリプト
│   └── その他ユーティリティスクリプト
└── README.md - このドキュメント
```

## 2. テストデータの配置

処理したいCSVなどのデータファイルを `./data/input/csv/` ディレクトリに配置します：

```bash
# 作業ディレクトリに、データを配置しておく
cp *.csv ./data/input/csv/
```

MinIO対応版を使用する場合、データは自動的にMinIOにアップロードされます。手動でアップロードする場合は以下のコマンドを使用します：

```bash
./scripts/upload_csvs_python.sh ./data/input/csv/
```

以下のデータ形式がサポートされています：

- CSV (カンマ区切り)
- JSON (一行ずつのJSON形式)

ヘッダー行を含むCSVファイルを推奨します。

## 3. 処理の実行

### 従来版（ローカルファイルシステム使用）

#### 3.1 データカタログへの登録 (クローラー実行)

```bash
# クローラーを実行してデータをHive Metastoreに登録
chmod +x scripts/run_crawler.sh
./scripts/run_crawler.sh
```

#### 3.2 登録されたテーブルの確認

```bash
# Hiveを使用して登録されたテーブルを確認
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"
```

あるいは以下のコマンドでレコードも確認できます：

```bash
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.input_ap LIMIT 10;"
```

#### 3.3 Glueジョブの実行（従来版）

```bash
# 従来版Glueジョブの実行
chmod +x scripts/run_container_job.sh
./scripts/run_container_job.sh
```

プロンプトに従って実行するジョブ（list_ap、connection、または両方）と処理対象年月を入力します。

処理結果は以下のディレクトリに出力されます：
- `./data/output/list_ap_output/`  # list_apジョブの出力
- `./data/output/connection_output/`  # connectionジョブの出力

### MinIO S3対応版

#### 3.1 S3バケットの確認

MinIOが正しく動作していることを確認します：

```bash
# MinIOへの接続テスト
./scripts/minio_client.sh list_buckets
```

バケット内のオブジェクトを確認します：

```bash
# バケット内のオブジェクトを表示
./scripts/minio_client.sh list_objects glue-bucket
```

#### 3.2 MinIO対応ジョブの実行

```bash
# MinIO対応Glueジョブの実行
chmod +x scripts/run_minio_container_job_python.sh
./scripts/run_minio_container_job_python.sh
```

プロンプトに従って実行するジョブ（list_ap、connection、または両方）と処理対象年月を入力します。

処理結果は以下の場所に出力されます：

**ローカルファイルシステム:**
- `./data/output/list_ap_output/`  # list_apジョブの出力
- `./data/output/connection_output/`  # connectionジョブの出力

**MinIO S3:**
- `s3://glue-bucket/output/list_ap_output/`  # list_apジョブの出力
- `s3://glue-bucket/output/connection_output/`  # connectionジョブの出力

MinIO S3内のデータを確認するには：

```bash
# MinIO内の出力を確認
./scripts/minio_client.sh list_objects glue-bucket output/list_ap_output/
```

## 4. MinIO管理コンソールへのアクセス

MinIOには管理用のWebインターフェースが用意されています。ブラウザから以下のURLにアクセスできます：

- URL: http://localhost:9001
- ユーザー名: minioadmin
- パスワード: minioadmin

このコンソールから、バケットの作成、ファイルのアップロード・ダウンロード、アクセス権の管理などが可能です。

## 5. 独自のGlueジョブの実装

### 従来版（ローカルファイルシステム使用）

1. `jobs/` ディレクトリに新しいPythonスクリプトを作成します。
2. 既存のジョブ（`list_ap_job.py` または `connection_job.py`）を参考にしてください。
3. モックの GlueContext や DynamicFrame クラスを使用してください。

### MinIO S3対応版

1. `jobs/` ディレクトリに新しいPythonスクリプトを作成します（`minio_` プレフィックスを付けることを推奨）。
2. 既存のジョブ（`minio_list_ap_job.py` または `minio_connection_job.py`）を参考にしてください。
3. S3クライアントの初期化には次のようなコードを使用します：

```python
# S3クライアントの設定（環境変数から読み込む）
def get_s3_client():
    endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    region_name = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=region_name
    )
```

バケット名は `BUCKET_NAME.py` で管理されています。以下のように使用できます：

```python
import BUCKET_NAME

# ローカル環境のバケット名
bucket_name = BUCKET_NAME.LOCAL_BUCKET

# 本番環境のバケット名
# bucket_name = BUCKET_NAME.OPEN  # 外部公開データ用
# bucket_name = BUCKET_NAME.MIDDLE  # 中間データ用
# bucket_name = BUCKET_NAME.MASTER  # マスターデータ用
# bucket_name = BUCKET_NAME.INITIAL  # 初期データ用
```

## 6. トラブルシューティング

### AWS CLI/botocore のバージョン不整合

AWS CLIとbotocoreのバージョン不整合が発生した場合は、以下のスクリプトを実行して修正してください：

```bash
./scripts/fix_aws_versions.sh
```

### MinIO接続の問題

MinIOへの接続に問題がある場合は、以下のスクリプトを実行して接続をテストします：

```bash
./scripts/test_minio_connection.sh
```

### 権限の問題

ファイルアクセス権限に問題がある場合は以下を実行：

```bash
chmod -R 777 ./data ./hive-data ./trino-data ./minio-data
./scripts/fix_permissions.sh
```

### コンテナの再起動

問題が発生した場合はコンテナを再起動して状態をリセット：

```bash
docker-compose down
docker-compose up -d
```

### ログの確認

各コンテナのログを確認：

```bash
docker logs glue20250407
docker logs minio_20250407
docker logs hive_metastore_20250407
docker logs mysql_20250407
docker logs trino_20250407
```

## 7. 環境のクリーンアップ

使用が終了したら環境を削除できます：

```bash
# コンテナと関連ネットワークを停止・削除
docker-compose down

# 生成されたデータを削除（オプション）
rm -rf ./data/output ./mysql-data ./hive-data ./trino-data ./minio-data
```

## 8. 高度な使用方法

### 8.1 カスタムS3パスの設定

MinIO S3のパス構造は `BUCKET_NAME.py` モジュールで定義されています。本番環境との互換性のために、必要に応じてこのファイルを編集してパス構造を調整できます。

### 8.2 Jupyter Labの使用

Jupyter Labは以下のURLでアクセスできます：
http://localhost:8888

Jupyter Lab環境では、Sparkを使用したデータの探索やモデルの開発が可能です。

### 8.3 Trinoクエリエンジンの使用

Trinoクエリエンジンは以下のURLでアクセスできます：
http://localhost:8080

SQLクエリを実行することでデータの探索や分析が可能です：

```bash
# SQLファイルの作成
echo "SELECT * FROM test_db_20250407.input_ap LIMIT 10;" > query.sql

# SQLファイルの実行
./scripts/execute_sql.sh query.sql
```

## 9. 本番環境との違い

このエミュレータは本番環境の動作を可能な限り再現していますが、以下のような違いがあります：

- **ファイルパスの違い**: 本番環境ではS3パスを使用しますが、従来版エミュレータではローカルファイルシステムを使用します。
- **権限管理**: 本番環境ではIAMを使用した権限管理がありますが、エミュレータでは簡易的な認証のみが実装されています。
- **スケーラビリティ**: 本番環境は高いスケーラビリティを持ちますが、エミュレータは単一のDockerホスト上で動作するため制限があります。
- **サービス連携**: AWS LambdaやStep Functionsなど他のAWSサービスとの連携機能は限定的です。

MinIO S3対応版は、よりS3に近い動作を再現していますが、完全に同一の動作を保証するものではありません。

## 10. 補足情報

### 使用しているコンテナイメージ

- Glue: amazon/aws-glue-libs:0.0_image_01
- MinIO: minio/minio
- Hive Metastore: apache/hive:3.1.3
- MySQL: mysql:5.7
- Trino: trinodb/trino

### スクリプト一覧

| スクリプト | 説明 |
|-----------|------|
| setup.sh | 従来版環境の初期セットアップ |
| scripts/init_minio_python.sh | MinIO対応環境の初期セットアップ |
| scripts/run_container_job.sh | 従来版Glueジョブの実行 |
| scripts/run_minio_container_job_python.sh | MinIO対応Glueジョブの実行 |
| scripts/run_crawler.sh | クローラーの実行 |
| scripts/minio_client.sh | MinIOクライアントコマンド |
| scripts/test_minio_connection.sh | MinIO接続テスト |
| scripts/fix_permissions.sh | 権限修正 |

### 推奨される使用方法

- 開発初期段階では従来版で素早く開発
- 本番環境への移行前にMinIO対応版でテスト
- 継続的インテグレーション（CI）環境ではMinIO対応版を使用