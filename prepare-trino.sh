#!/bin/bash

# Trinoのデータディレクトリを作成し、適切な権限を設定
mkdir -p ./trino-data
chmod -R 777 ./trino-data

# 必要なサブディレクトリを作成
mkdir -p ./trino-data/var/log
mkdir -p ./trino-data/var/run
chmod -R 777 ./trino-data/var
