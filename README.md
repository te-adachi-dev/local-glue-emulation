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

## 1. セットアップ

### 前提条件

- Docker
- Docker Compose
- Git

### インストール

```bash
git clone https://github.com/yourusername/local-glue-emulation.git
cd local-glue-emulation
```

### 環境の構築と起動

```bash
# セットアップスクリプトを実行
chmod +x setup.sh
./setup.sh
```

このスクリプトは以下の処理を行います：
- 必要なディレクトリの作成
- 権限の設定
- Dockerコンテナの起動

起動には数分かかる場合があります。以下のコマンドでコンテナの状態を確認できます：

```bash
docker-compose ps
```

## 2. テストデータの配置

処理したいCSVなどのデータファイルを `./data/input/csv/` ディレクトリに配置します：

```bash
# 例: サンプルCSVを配置
cp /path/to/your/data/*.csv ./data/input/csv/
```

以下のデータ形式がサポートされています：
- CSV (カンマ区切り)
- JSON (一行ずつのJSON形式)

ヘッダー行を含むCSVファイルを推奨します。

## 3. 処理の実行

### 3.1 データカタログへの登録 (クローラー実行)

```bash
# クローラーを実行してデータをHive Metastoreに登録
chmod +x run_crawler.sh
./run_crawler.sh
```

### 3.2 登録されたテーブルの確認

```bash
# Hiveを使用して登録されたテーブルを確認
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"
```

あるいは以下のコマンドでレコードも確認できます：

```bash
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.テーブル名 LIMIT 10;"
```

### 3.3 Glueジョブの実行

サンプルGlueジョブを実行して、登録されたデータを処理します：

```bash
# テストジョブの実行
docker exec glue20250407 python3 /home/glue_user/workspace/jobs/test_job_hive.py --database test_db_20250407 --table テーブル名
```

処理結果は以下のディレクトリに出力されます：
```
./data/output/processed_data/
```

### 3.4 独自のGlueジョブの実行

独自のGlueジョブスクリプトを実行する場合：

```bash
# スクリプトをコンテナにコピー
docker cp your_job_script.py glue20250407:/home/glue_user/workspace/jobs/

# ジョブを実行
docker exec glue20250407 python3 /home/glue_user/workspace/jobs/your_job_script.py --database test_db_20250407 --table テーブル名
```

## トラブルシューティング

### 権限の問題

ファイルアクセス権限に問題がある場合は以下を実行：

```bash
chmod -R 777 ./data ./hive-data ./trino-data
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
docker logs hive_metastore_20250407
docker logs mysql_20250407
docker logs trino_20250407
```

### Hiveメタストア接続エラー

Hiveメタストアに接続できない場合：

```bash
# Hiveメタストアの状態確認
docker exec hive_metastore_20250407 ps -ef | grep hive

# 必要に応じて再起動
docker-compose restart hive-metastore
```

## 環境のクリーンアップ

使用が終了したら環境を削除できます：

```bash
# コンテナと関連ネットワークを停止・削除
docker-compose down

# 生成されたデータを削除（オプション）
rm -rf ./data ./mysql-data ./hive-data ./trino-data
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
├── scripts/ - 各種操作用スクリプト
├── setup.sh - 環境セットアップスクリプト
└── run_crawler.sh - クローラー実行スクリプト
```
