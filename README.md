# ローカルAWS Glueモック環境

以下の機能をローカル環境で再現し、ローカルでのテスト等でご利用いただけます。
・Glueデータカタログ
・Glueジョブ

## 主な機能

- **S3代替**: ローカルディレクトリでS3バケットを代用
- **Glueデータカタログ**: Hiveメタストアを使った疑似Glueデータカタログ
- **クローラー**: ローカルファイルを検出してテーブル定義を自動生成
- **Glueジョブ**: PySpark/Glueジョブをローカルで実行
- **SQL実行**: 登録されたテーブルに対してSQLクエリを実行

## 起動方法

### 前提条件

- Docker と Docker Compose がインストールされていること
- Gitがインストールされていること

### セットアップ手順

1. リポジトリをクローン
```bash
git clone https://github.com/te-adachi-dev/local-glue-emulation.git
cd local-glue-emulation
```

2. 環境のセットアップを実行
```bash
./setup.sh
```
テストデータの準備、ディレクトリ構造の作成、Dockerコンテナの設定を行います。

3. Docker Composeでコンテナを起動
```bash
docker-compose up -d
```
または以下のスクリプトを使用することもできます：
```bash
./scripts/start_local_env.sh
```

4. 動作確認
```bash
./healthcheck.sh
```
全てのコンテナが正常に起動しているか確認します。

5. テストデータの検証
```bash
./verify.sh
```
テストデータベースとテーブルが正しく登録されているか確認します。

## ファイル登録方法

新しいデータファイルを環境に登録する方法は以下の通りです。

1. CSVファイルを入力ディレクトリに配置
```bash
# CSVファイルをコピー
cp test_data.csv ./data/input/csv/
```

2. ウェアハウスディレクトリにもコピー（手動でテーブル定義する場合）
```bash
mkdir -p ./data/warehouse/test_table_name
cp ./data/input/csv/test_data.csv ./data/warehouse/test_table_name/
```

3. 新しいCSVファイルのヘッダー情報が正しいことを確認
   - 1行目はカラム名として使用されます
   - カラム名に特殊文字や空白が含まれていないことを確認

## 疑似クローラ実行方法

クローラーを実行してデータファイルを検出し、テーブル定義を自動生成します：

```bash
./scripts/run_crawler.sh
```

このスクリプトは以下の処理を行います：
- 入力ディレクトリ内のCSVファイルを検出
- ファイル内容からスキーマを推測
- Hiveテーブル定義を生成
- Hiveメタストアにテーブルを登録

クローラー実行後、自動生成されたDDLファイルが `./temp_ddl.hql` に保存されます。

## 疑似Glueジョブ登録方法

### 1. ジョブスクリプトの作成

新しいジョブスクリプトを `jobs` ディレクトリに配置します：

```bash
vi ./jobs/test_job_name.py
```

ジョブスクリプト例：
```python
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

# Glueジョブ初期化
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder \
    .appName("testJobName") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/opt/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# データベースとテーブル名の指定
database = "test_db_20250407"
table = "test_table_name"

# テーブルからデータ読み込み
df = spark.sql(f"SELECT * FROM {database}.{table}")
print("データ読み込み成功:")
df.show()

# データ変換処理
processed_df = df.withColumn("new_column", df["existing_column"] * 2)

# 結果の保存
output_path = "/home/glue_user/workspace/data/output/test_output_directory"
processed_df.write.mode("overwrite").parquet(output_path)
print(f"処理結果を {output_path} に保存しました")
```

### 2. ジョブ実行

以下のコマンドで作成したジョブを実行します：

```bash
# run_job.shを修正して新しいジョブを指定
vi ./scripts/run_job.sh

# 修正例（docker execコマンドのパスとパラメータを変更）:
# docker exec glue20250407 bash -c "cd /home/glue_user/workspace && python3 /home/glue_user/workspace/jobs/test_job_name.py --JOB_NAME test_job_name --database test_db_20250407 --table test_table_name"

# 修正後、ジョブを実行
./scripts/run_job.sh
```

または、以下のようにDockerコンテナ内で直接ジョブを実行することもできます：

```bash
docker exec glue20250407 python3 /home/glue_user/workspace/jobs/test_job_name.py --JOB_NAME test_job_name --database test_db_20250407 --table test_table_name
```

## 出力ファイル確認 / 出力データSQL実行方法

### 出力ファイルの確認

ジョブによって生成された出力ファイルは `./data/output/` ディレクトリに保存されます：

```bash
# 出力ディレクトリのリスト表示
ls -la ./data/output/

# 特定の出力ディレクトリの内容確認
ls -la ./data/output/test_output_directory/
```

### SQL実行方法

登録されたテーブルに対してSQLクエリを実行するには：

```bash
# SQL実行（結果をコンソールに表示）
./scripts/execute_sql.sh "SELECT * FROM test_db_20250407.test_table_name LIMIT 10"

# SQL実行（結果をCSVファイルに保存）
./scripts/execute_sql.sh "SELECT * FROM test_db_20250407.test_table_name" results.csv
```

保存されたCSVファイルは `./data/output/query_results/` ディレクトリに格納されます。

### Jupyter Notebookでの分析

この環境にはJupyter Notebookも組み込まれています。以下のURLで開くことができます：

```
http://localhost:8888
```

Jupyter Notebookでのデータ分析例：

```python
# 必要な環境変数を設定
import os
os.environ['AWS_GLUE_CATALOG_FAKE_REGION'] = 'us-east-1'
os.environ['DISABLE_AWS_GLUE'] = 'true'
os.environ['DISABLE_SSL'] = 'true'

# Spark環境のセットアップ
from pyspark import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

# CSVファイルを直接読み込む
csv_path = "/home/glue_user/workspace/data/warehouse/test_table_name/test_data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
df.show()

# データ分析
from pyspark.sql.functions import col
result = df.groupBy("category_column").agg({"numeric_column": "sum"})
result.show()
```

## トラブルシューティング

### コンテナが起動しない場合

```bash
# ログを確認
docker-compose logs

# 特定のコンテナのログを確認
docker-compose logs hive-metastore
```

### データベースやテーブルが見つからない場合

```bash
# Hiveメタストアの状態を確認
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "SHOW DATABASES;"
docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "USE test_db_20250407; SHOW TABLES;"

# クローラーを再実行
./scripts/run_crawler.sh
```

### パスの問題が発生した場合

コンテナ内のパスとホスト側のパスが異なる場合があります。パスの先頭に `/home/glue_user/workspace/` を追加することで解決できる場合があります。

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は [LICENSE](LICENSE) ファイルを参照してください。
