# ローカルGlue Mockエミュレーション環境

Dockerコンテナを使用せず、PythonとPySparkだけでAWS Glueの機能をローカルでシミュレートする環境です。

## 機能

- AWS Glueのクローラー、ジョブ、SQLクエリ機能をローカルでエミュレート
- ファイルからのデータ読み込みと一時テーブルの作成
- ローカルStandaloneのSparkセッションによるETL処理
- スケジュールされたGlueジョブの動作をテスト
- WindowsおよびLinux環境に対応

## 前提条件

- Python 3.7以上
- Java 8以上（Sparkの実行に必要）
- Hadoopバイナリ（Windows環境のみ）

## インストール方法

1. 必要なPythonパッケージをインストール
```bash
pip install pyspark==3.1.2 findspark pandas numpy pyarrow
```

2. Hadoopバイナリのセットアップ
```bash
# Hadoopバイナリを配置（例：C:\tools\hadoop-2.7.2）
# 環境変数の設定
setx HADOOP_HOME C:\tools\hadoop-2.7.2
setx PATH "%PATH%;%HADOOP_HOME%\bin"
```
3. JDKのセットアップ
```bash
# https://download.oracle.com/otn/java/jdk/11.0.26%2B7/720377fa814b4b4491dea5837af261de/jdk-11.0.26_windows-x64_bin.exe
# 環境変数の設定
setx /M JAVA_HOME "C:\Program Files\Java\jdk-11"
setx /M Path "%JAVA_HOME%\bin;%Path%"
```


## 使用方法

### 環境セットアップ
```bash
python run_local_glue.py setup
```

### クローラーの実行
任意のディレクトリにCSVファイルを配置し、クローラーを実行してテーブル定義を作成します。

```bash
# CSVファイルを配置
cp データ.csv ./data/input/csv/

# クローラーを実行
python run_local_glue.py crawler --input_path ./data/input/csv
```

### SQLクエリの実行
テーブルに対してSQLクエリを実行します。

```bash
# テーブル一覧の表示
python run_local_glue.py sql --query "SHOW TABLES"

# テーブルデータの表示
python run_local_glue.py sql --query "SELECT * FROM テーブル名 LIMIT 10"

# クエリ結果のファイル出力
python run_local_glue.py sql --query "SELECT * FROM テーブル名" --output ./data/output/query_results/結果.csv
```

### Glueジョブの実行
ETLジョブを実行します。ジョブスクリプトは `jobs` ディレクトリに配置します。

```bash
python run_local_glue.py job --job_script ./jobs/test_job.py --job_name ジョブ名 --table テーブル名
```

## ジョブスクリプトの例
以下はETLジョブの基本テンプレートです：

```python
# jobs/sample_job.py
import sys
from pyspark.sql.functions import col

# 引数を取得（ローカルGlueランナーから渡される）
try:
    # argsとjobはすでに定義されているか確認
    print(f"Running with table: {args['table']}")
except NameError:
    # 定義されていない場合はデフォルト値を設定
    args = {
        'table': 'テーブル名'
    }
    from local_spark_session import JobMock, get_spark_session
    spark, sc, glue_context = get_spark_session("SampleJob")
    job = JobMock(glue_context)
    job.init("sample_job", args)

# データの読み込み
df = spark.sql(f"SELECT * FROM {args['table']}")

# データ変換処理
transformed_df = df.withColumn("new_column", col("existing_column") + 1)

# 結果の保存
output_path = f"./data/output/processed_{args['table']}"
transformed_df.write.mode("overwrite").parquet(output_path)

# 処理したデータをテーブルとして登録
transformed_df.createOrReplaceTempView(f"{args['table']}_processed")

# ジョブ完了
print("Job completed successfully!")
job.commit()
```

## 制限事項
* セッション間でのテーブル永続化は行われません（メモリ内テーブル）
* 複雑なGlueカタログ機能はサポートされていません
* AWS固有の機能（IAM、クロスアカウント等）は使用できません

## ライセンス
MIT