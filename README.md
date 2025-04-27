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
git clone https://github.com/te-adachi-dev/local-glue-emulation.git
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
# 作業ディレクトリに、データを配置しておく。
# 以下コマンドで配置できる。
cp *.csv ./data/input/csv/
```

以下のデータ形式がサポートされています：

- CSV (カンマ区切り)
- JSON (一行ずつのJSON形式)

ヘッダー行を含むCSVファイルを推奨します。

## 3. 処理の実行

### 3.1 データカタログへの登録 (クローラー実行)

```bash
# クローラーを実行してデータをHive Metastoreに登録
chmod +x scripts/run_crawler.sh
./scripts/run_crawler.sh
```

### 3.2 登録されたテーブルの確認

```bash
# Hiveを使用して登録されたテーブルを確認
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"
```

あるいは以下のコマンドでレコードも確認できます：

```bash
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT * FROM test_db_20250407.input_ap LIMIT 10;"
```

### 3.3 Glueジョブの実行

Glueジョブを実行するには、実行用のコンテナスクリプトを使用します：

```bash
# Glueジョブの実行
chmod +x scripts/run_container_job.sh
./scripts/run_container_job.sh
```

プロンプトに従って実行するジョブ（list_ap、connection、または両方）と処理対象年月を入力します。

処理結果は以下のディレクトリに出力されます：

```
./data/output/list_ap_output/  # list_apジョブの出力
./data/output/connection_output/  # connectionジョブの出力
```

### 3.4 独自のGlueジョブの実行

独自のGlueジョブスクリプトを実装する場合：

1. jobs/ディレクトリにPythonスクリプトを作成
2. コンテナ実行スクリプトを修正してジョブを登録
3. 実行スクリプトを使用してジョブを実行

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
│   ├── list_ap_job.py - APリスト処理ジョブ
│   └── connection_job.py - 接続データ処理ジョブ
├── scripts/ - 各種操作用スクリプト
│   ├── run_crawler.sh - クローラー実行スクリプト
│   ├── run_container_job.sh - Glueジョブ実行スクリプト
│   └── execute_sql.sh - SQL実行スクリプト 
├── setup.sh - 環境セットアップスクリプト
└── README.md - このドキュメント
```

## サンプルクエリ

クロールしたテーブルに対してHiveから直接クエリを実行する例：

```bash
# input_apテーブルからAPデータを検索
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT apid, ap名称, 設置場所名称, 都道府県, 市区町村 FROM test_db_20250407.input_ap WHERE yearmonth = 202501 LIMIT 5;"

# 都道府県別のAP数を集計
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT 都道府県, COUNT(*) as ap_count FROM test_db_20250407.input_ap GROUP BY 都道府県 ORDER BY ap_count DESC;"

# 接続データを検索
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT apid, 利用開始日時, 利用者属性, 端末属性 FROM test_db_20250407.input_connection WHERE yearmonth = 202501 LIMIT 5;"

# マスターデータと結合して検索
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SELECT c.apid, c.利用開始日時, d.共通ログ端末属性 FROM test_db_20250407.input_connection c JOIN test_db_20250407.master_device_attribute d ON c.端末属性 = d.各社端末属性 LIMIT 5;"
```

また、SQLクエリをファイルから実行することも可能です：

```bash
# SQLファイルの作成
echo "SELECT * FROM test_db_20250407.input_ap LIMIT 10;" > query.sql
# SQLファイルの実行
docker exec -i hive_metastore_20250407 /opt/hive/bin/hive -f - < query.sql
```
