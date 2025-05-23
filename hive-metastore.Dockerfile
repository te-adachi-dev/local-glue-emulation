FROM apache/hive:3.1.3

USER root

# 必要なパッケージのインストール (mysql-clientの代わりにmariadb-clientを使用)
RUN apt-get update && \
    apt-get install -y wget netcat procps net-tools mariadb-client && \
    wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar -O /opt/hive/lib/mysql-connector-java-8.0.27.jar

# 作業ディレクトリ作成と権限設定
RUN mkdir -p /tmp/hive && chmod 777 /tmp/hive && \
    mkdir -p /tmp/hadoop-hive && chmod 777 /tmp/hadoop-hive && \
    mkdir -p /opt/hive/warehouse && chmod 777 /opt/hive/warehouse && \
    mkdir -p /opt/hive/data && chmod 777 /opt/hive/data

# ~/.beeline ディレクトリ作成
RUN mkdir -p /home/hive/.beeline && chmod 777 /home/hive/.beeline

# hive-site.xmlを準備
COPY conf/hive-conf/hive-site.xml /opt/hive/conf/hive-site.xml

# エントリポイントスクリプト作成
RUN echo '#!/bin/bash\n\
sleep 15\n\
# Mysqlが利用可能になるまで待機\n\
while ! nc -z mysql 3306; do\n\
  echo "Waiting for MySQL..."\n\
  sleep 3\n\
done\n\
echo "MySQL is up, initializing schema..."\n\
\n\
# スキーマを初期化（すでに存在する場合はスキップ）\n\
${HIVE_HOME}/bin/schematool -dbType mysql -initSchema || echo "Schema may already exist"\n\
\n\
# Hiveメタストアサービスを起動\n\
exec ${HIVE_HOME}/bin/hive --service metastore\n\
' > /opt/hive/start-metastore.sh && chmod +x /opt/hive/start-metastore.sh

USER hive
ENTRYPOINT ["/opt/hive/start-metastore.sh"]