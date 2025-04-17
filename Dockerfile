FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# 必要なツールをインストール
RUN yum update -y && yum install -y nc procps-ng

# Python3を優先的に使用するよう設定
RUN echo 'alias python=python3' >> /home/glue_user/.bashrc

# AWS Glue設定用のスクリプトを作成
RUN echo 'export PYTHONIOENCODING=utf8' >> /home/glue_user/.bashrc
RUN echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> /home/glue_user/.bashrc
RUN echo 'export DISABLE_SSL=true' >> /home/glue_user/.bashrc
RUN echo 'export DISABLE_AWS_GLUE=true' >> /home/glue_user/.bashrc

# Sparkの設定を更新
RUN echo 'spark.hadoop.hive.metastore.client.factory.class=' >> /home/glue_user/spark/conf/spark-defaults.conf
RUN echo 'spark.sql.warehouse.dir=/opt/hive/warehouse' >> /home/glue_user/spark/conf/spark-defaults.conf

# glue_userに戻す
USER glue_user