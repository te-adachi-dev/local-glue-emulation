FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# 必要なパッケージのインストール
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    iputils-ping \
    net-tools \
    vim \
    netcat \
    jq \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# AWS CLIとboto3のインストール
RUN pip3 install --upgrade awscli boto3

# タイムゾーンの設定
RUN ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime

# 作業ディレクトリの設定と権限付与
RUN mkdir -p /home/glue_user/data/input/csv \
    && mkdir -p /home/glue_user/data/output \
    && mkdir -p /home/glue_user/workspace/jobs \
    && mkdir -p /home/glue_user/crawler \
    && chmod -R 777 /home/glue_user

# AWS CLIの初期設定
RUN mkdir -p /home/glue_user/.aws \
    && echo "[default]\naws_access_key_id = minioadmin\naws_secret_access_key = minioadmin" > /home/glue_user/.aws/credentials \
    && echo "[default]\nregion = ap-northeast-1\ns3 =\n    signature_version = s3v4\n    addressing_style = path" > /home/glue_user/.aws/config \
    && chown -R glue_user:glue_user /home/glue_user/.aws

# 環境変数を設定
ENV AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    AWS_DEFAULT_REGION=ap-northeast-1

# bashrcに環境変数を追加
RUN echo 'export AWS_ACCESS_KEY_ID=minioadmin' >> /home/glue_user/.bashrc \
    && echo 'export AWS_SECRET_ACCESS_KEY=minioadmin' >> /home/glue_user/.bashrc \
    && echo 'export AWS_DEFAULT_REGION=ap-northeast-1' >> /home/glue_user/.bashrc

USER glue_user
WORKDIR /home/glue_user/workspace

# JupyterLabをデフォルトで起動するよう設定
ENV JUPYTER_ENABLE_LAB=yes