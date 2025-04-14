import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Glueジョブ初期化
sc = SparkContext()
glueContext = GlueContext(sc)

# 引数の取得
if len(sys.argv) > 1:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database', 'table'])
else:
    args = {
        'JOB_NAME': 'local_glue_job',
        'database': 'test_db_20250407',
        'table': 'test_table'
    }

# Sparkセッションの初期化
spark = SparkSession.builder \
    .appName(args['JOB_NAME']) \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/home/glue_user/workspace/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # CSVファイルから直接読み込む方法も試す
    print("Attempting to read CSV file directly...")
    csv_path = "/home/glue_user/workspace/data/input/csv/test_table.csv"
    if os.path.exists(csv_path):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print("Successfully read data from CSV file")
    else:
        print(f"CSV file not found at {csv_path}")
        raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
except Exception as e:
    print(f"Error reading CSV: {e}")
    print("Falling back to MySQL access")

    # MySQLから直接読み込む（フォールバック）
    jdbc_url = "jdbc:mysql://mysql:3306/test_db_20250407"
    connection_properties = {
        "user": "hive",
        "password": "hive",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df = spark.read.jdbc(url=jdbc_url, table="test_table", properties=connection_properties)
        print("Successfully read data from MySQL")
    except Exception as e:
        print(f"Error reading from MySQL: {e}")
        print("Creating sample data manually")
        
        # サンプルデータを手動で作成（最後の手段）
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("department", StringType(), True)
        ])
        
        data = [
            (1, "テスト太郎", 30, "開発部"),
            (2, "テスト花子", 25, "営業部"),
            (3, "テスト次郎", 40, "総務部"),
            (4, "テスト三郎", 35, "人事部")
        ]
        
        df = spark.createDataFrame(data, schema)

# データの表示
print("Data sample:")
df.show(10)

# ここでデータ変換やビジネスロジックを実装
print("Processing data...")
processed_df = df.withColumn("department_code", df.department.substr(0, 1))

# 結果の表示
print("Processed data:")
processed_df.show(10)

# 結果をファイルとして保存（S3に相当）
output_path = "/home/glue_user/workspace/data/output/processed_data"
print(f"Saving results to {output_path}")
processed_df.write.mode("overwrite").parquet(output_path)

print("Job completed successfully!")
job.commit()