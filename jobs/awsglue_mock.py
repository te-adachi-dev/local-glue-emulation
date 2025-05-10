#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AWS Glueライブラリのモック実装
ホスト側でのテスト実行時に使用します
"""

import sys
import os

# モックのgetResolvedOptions関数
def getResolvedOptions(argv, args):
    result = {}
    # デフォルト値
    result['JOB_RUN_ID'] = 'mock_run_001'
    
    # 引数を解析
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith('--'):
            arg_name = arg[2:]  # --を除去
            if i + 1 < len(argv) and not argv[i+1].startswith('--'):
                result[arg_name] = argv[i+1]
                i += 2
            else:
                result[arg_name] = ''
                i += 1
        else:
            i += 1
    
    # 必須引数のチェック
    for arg in args:
        if arg not in result:
            raise ValueError(f"必須引数 {arg} が指定されていません")
    
    # 引数をクラスのように扱えるよう、辞書をオブジェクトに変換
    class Args:
        pass
    
    args_obj = Args()
    for key, value in result.items():
        setattr(args_obj, key, value)
    
    return args_obj

# モックのDynamicFrame
class DynamicFrame:
    def __init__(self, dataframe, context, name):
        self.dataframe = dataframe
        self.context = context
        self.name = name
    
    def toDF(self):
        return self.dataframe
    
    @classmethod
    def fromDF(cls, dataframe, context, name):
        return cls(dataframe, context, name)

# モックのDynamicFrameCollection
class DynamicFrameCollection:
    def __init__(self, frames, context):
        self.frames = frames
        self.context = context
    
    def keys(self):
        return list(self.frames.keys())
    
    def select(self, key):
        return self.frames[key]

# モックのJob
class Job:
    def __init__(self, context):
        self.context = context
    
    def init(self, name, args):
        self.name = name
        self.args = args
    
    def commit(self):
        print(f"Job {self.name} committed")

# モックのGlueContext
class GlueContext:
    def __init__(self, spark_context):
        self.spark_context = spark_context
        self.spark_session = getattr(spark_context, 'getOrCreate', lambda: None)()
    
    def create_dynamic_frame_from_catalog(self, database, table_name, transformation_ctx):
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.getOrCreate()
        return None
    
    def create_dynamic_frame_from_options(self, format_options, connection_type, format, connection_options, transformation_ctx):
        return None
    
    def write_dynamic_frame_from_options(self, frame, connection_type, format, connection_options, format_options=None):
        pass
    
    @property
    def write_dynamic_frame(self):
        class FromOptions:
            def __init__(self, glue_ctx):
                self.glue_ctx = glue_ctx
                
            def from_options(self, frame, connection_type, format, connection_options, format_options=None):
                self.glue_ctx.write_dynamic_frame_from_options(
                    frame=frame,
                    connection_type=connection_type,
                    format=format,
                    connection_options=connection_options,
                    format_options=format_options
                )
        
        return FromOptions(self)

# 変換関数のモック
class SelectFromCollection:
    @staticmethod
    def apply(dfc, key, transformation_ctx):
        return dfc.select(key)

class ApplyMapping:
    @staticmethod
    def apply(frame, mappings, transformation_ctx):
        df = frame.toDF()
        # カラム名のマッピングを適用
        for src_col, src_type, tgt_col, tgt_type in mappings:
            if src_col in df.columns:
                df = df.withColumnRenamed(src_col, tgt_col)
        return DynamicFrame(df, frame.context, transformation_ctx)

# モジュールとして使用できるようにする
utils = sys.modules[__name__]
sys.modules['awsglue.utils'] = utils
sys.modules['awsglue.context'] = sys.modules[__name__]
sys.modules['awsglue.job'] = sys.modules[__name__]
sys.modules['awsglue.dynamicframe'] = sys.modules[__name__]
sys.modules['awsglue'] = sys.modules[__name__]
