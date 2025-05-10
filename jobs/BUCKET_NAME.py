# 本番環境のバケット名
OPEN = "ap-open-bucket"  # 外部公開データ用バケット
MIDDLE = "ap-middle-bucket"  # 中間データ用バケット
MASTER = "ap-master-bucket"  # マスターデータ用バケット
INITIAL = "ap-initial-bucket"  # 初期データ用バケット

# ローカル環境のバケット名（MinIO）
LOCAL_BUCKET = "glue-bucket"

# マスターデータのパス（本番環境）
MASTER_DEVICE_PATH = "device_attribute/"
MASTER_STATUS_PATH = "status/"
MASTER_USER_PATH = "user_attribute/"

# 入力データのパス（本番環境）
INITIAL_BASE_PATH = "input/input_connection/"

# 出力データのパス（本番環境）
MIDDLE_BASE_PATH = "common/common_connection/"
