-- テストデータベース作成
CREATE DATABASE IF NOT EXISTS test_db_20250407;

-- hiveユーザーにすべての権限を付与
GRANT ALL PRIVILEGES ON test_db_20250407.* TO 'hive'@'%';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';

-- 権限を反映
FLUSH PRIVILEGES;
