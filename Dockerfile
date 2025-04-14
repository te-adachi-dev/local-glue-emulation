FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# 必要なツールをインストール (Amazon Linuxにはyumを使用)
RUN yum update -y && yum install -y nc procps-ng

# Python3を優先的に使用するよう設定
RUN echo 'alias python=python3' >> /home/glue_user/.bashrc

# spark-submitへのシンボリックリンク作成
RUN ln -sf /usr/bin/spark-submit /usr/local/bin/spark-submit

# glue_userに戻す
USER glue_user