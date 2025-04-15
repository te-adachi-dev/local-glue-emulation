#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import tempfile
import findspark

# 一時ディレクトリの設定 - Windowsでの権限問題回避
os.environ["TMPDIR"] = tempfile.gettempdir()

# Hadoop環境変数の設定
hadoop_home = "C:/tools/hadoop-2.7.2"
os.environ["HADOOP_HOME"] = hadoop_home

# Windows固有の設定
if sys.platform.startswith('win'):
    # バックスラッシュをスラッシュに変換
    os.environ["HADOOP_HOME"] = os.environ["HADOOP_HOME"].replace("\\", "/")
    
    # JAVA_HOMEの設定
    if "JAVA_HOME" not in os.environ or not os.environ["JAVA_HOME"]:
        possible_java_homes = [
            "C:/Program Files/Java/jdk-11",
            "C:/Program Files/Java/jdk1.8.0_291",
            "C:/Program Files/Java/jre1.8.0_291"
        ]
        for path in possible_java_homes:
            if os.path.exists(path):
                os.environ["JAVA_HOME"] = path
                break
    
    # 重要: Python環境のためにhadoop.dllを確実にPATHに入れる
    hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    if hadoop_bin not in os.environ["PATH"]:
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]
    
    # Derby関連の設定 - 一時メタストア用
    derby_home = os.path.join(tempfile.gettempdir(), "derby")
    os.makedirs(derby_home, exist_ok=True)
    os.environ["DERBY_HOME"] = derby_home
    
    # シンプルモードでの環境設定
    os.environ["HADOOP_USER_NAME"] = "hive"
    os.environ["SPARK_LOCAL_DIRS"] = tempfile.gettempdir()
    os.environ["SPARK_WORKER_DIR"] = tempfile.gettempdir()
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Sparkのローカル設定 - Hiveサポートなし
findspark.init()

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

# プロジェクトのルートパス
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(ROOT_DIR, "data", "warehouse")
SPARK_WAREHOUSE_DIR = os.path.join(ROOT_DIR, "spark-warehouse")

# ローカルSparkセッションを取得する関数
def get_spark_session(app_name="GlueMockLocal"):
    """ローカルモードでSparkセッションを初期化 - シンプル版"""
    conf = SparkConf()
    conf.set("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR.replace("\\", "/"))
    conf.set("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=" + hadoop_home.replace("\\", "/"))
    
    # 最小限の設定でSparkを起動
    conf.set("spark.ui.enabled", "false")  # UIを無効化
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.master", "local[*]")
    
    try:
        # まずHiveサポートなしで試行
        spark = SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()
        
        # SparkとGlueの互換性対応のために必要な設定
        sc = spark.sparkContext
        glue_context = GlueContextMock(sc)
        
        return spark, sc, glue_context
    except Exception as e:
        print(f"通常のSpark初期化で失敗: {e}")
        print("最小限の設定でSparkを初期化します...")
        
        # 絶対最小限の設定でのSparkSession作成
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .getOrCreate()
        
        sc = spark.sparkContext
        glue_context = GlueContextMock(sc)
        
        return spark, sc, glue_context

# GlueContextのモック
class GlueContextMock:
    def __init__(self, spark_context):
        self.spark_context = spark_context
        
    def getSparkSession(self):
        return SparkSession.builder.getOrCreate()
    
    def create_dynamic_frame_from_options(self, connection_type, connection_options, format=None, format_options=None):
        """Glueの関数をシミュレート"""
        options = {}
        if format_options:
            options.update(format_options)
        if connection_options:
            options.update(connection_options)
        
        # 読み込み処理
        spark = self.getSparkSession()
        if connection_type == "s3":
            path = connection_options.get("paths")
            if isinstance(path, list):
                path = path[0]
            df = spark.read.format(format or "csv").options(**options).load(path)
        elif connection_type == "hive":
            database = connection_options.get("database")
            table_name = connection_options.get("table")
            try:
                df = spark.sql(f"SELECT * FROM {database}.{table_name}")
            except:
                # テーブルが見つからない場合はデータフレームを作成
                df = spark.createDataFrame([], schema=[])
        else:
            raise ValueError(f"接続タイプ {connection_type} はサポートされていません。")
        
        # DynamicFrameの代わりにDataFrameを返す
        return DynamicFrameMock(df)

# DynamicFrameのモック
class DynamicFrameMock:
    def __init__(self, dataframe):
        self.dataframe = dataframe
    
    def toDF(self):
        return self.dataframe
    
    def printSchema(self):
        return self.dataframe.printSchema()

# Jobクラスのモック
class JobMock:
    def __init__(self, glue_context):
        self.glue_context = glue_context
    
    def init(self, job_name, args=None):
        self.job_name = job_name
        self.args = args if args else {}
    
    def commit(self):
        print(f"Job '{self.job_name}' committed successfully.")

# 引数処理をシミュレート
def getResolvedOptions(argv, opt_list):
    """
    Glueの引数解決関数をモック
    """
    result = {}
    # デフォルト値
    for opt in opt_list:
        result[opt] = ""
    
    # コマンドライン引数の解析
    i = 0
    while i < len(argv):
        if argv[i].startswith('--'):
            opt_name = argv[i][2:]  # --を取り除く
            if i + 1 < len(argv) and not argv[i+1].startswith('--'):
                result[opt_name] = argv[i+1]
                i += 2
            else:
                result[opt_name] = True
                i += 1
        else:
            i += 1
    
    return result

# テスト用のメイン関数
if __name__ == "__main__":
    # Sparkセッションの初期化テスト
    try:
        spark, sc, glue_context = get_spark_session()
        
        # 基本的なテストの実行
        print("Sparkセッションが正常に初期化されました")
        print(f"Spark バージョン: {spark.version}")
        
        # シンプルなデータフレーム作成テスト
        data = [("テスト", 1), ("サンプル", 2)]
        df = spark.createDataFrame(data, ["名前", "値"])
        print("テストデータフレーム:")
        df.show()
        
        print("Sparkテスト完了")
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()