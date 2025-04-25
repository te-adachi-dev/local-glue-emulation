#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import random
from datetime import datetime, timedelta

# 出力ディレクトリの確認・作成
os.makedirs('./data/input/csv', exist_ok=True)
os.makedirs('./data/master/device_attribute', exist_ok=True)
os.makedirs('./data/master/user_attribute', exist_ok=True)
os.makedirs('./data/master/status', exist_ok=True)

# AP情報のサンプルデータ
ap_data = [
    ['apid', 'ap_name', 'location_name', 'prefecture', 'city', 'address', 'additional_info', 'latitude', 'longitude', 'status', 'use_start_datetime', 'use_end_datetime', 'category', 'owner_attr', 'facility_attr', 'station_attr', 'provider', 'yearmonth'],
    ['00:11:22:33:44:55', 'テストAP1', '新宿駅', '東京都', '新宿区', '西新宿1-1-1', '南口', 35.689634, 139.700464, '運用中', '2025/01/01 09:00', '2025/12/31 18:00', '交通', '公共', '駅', '通常', 'テスト事業者A', 202501],
    ['AA-BB-CC-DD-EE-FF', 'テストAP2', '渋谷駅', '東京都', '渋谷区', '道玄坂1-1-1', '東口', 35.658517, 139.701334, '運用中', '2025/01/01 09:00', '2025/12/31 18:00', '交通', '公共', '駅', '通常', 'テスト事業者B', 202501],
    ['ABCDEF123456', 'テストAP3', '池袋駅', '東京都', '豊島区', '南池袋1-28-1', '西口', 35.730255, 139.711068, '運用中', '2025/01/01 09:00', '2025/12/31 18:00', '交通', '公共', '駅', '通常', 'テスト事業者C', 202501]
]

# 接続情報のサンプルデータ
connection_data = [
    ['apid', 'use_start_datetime', 'user_attr', 'device_attr', 'provider', 'yearmonth'],
    ['00:11:22:33:44:55', '2025-01-22 09:15:30', 'ja', 'ios', 'テスト事業者A', 202501],
    ['AA-BB-CC-DD-EE-FF', '2025-01-22 10:30:45', 'ja-jp', 'android', 'テスト事業者B', 202501],
    ['ABCDEF123456', '2025-01-22 11:45:00', 'ja-gb', 'windows', 'テスト事業者C', 202501],
    ['00:11:22:33:44:55', '2025-01-22 12:00:15', 'ja-kr', 'macos', 'テスト事業者A', 202501],
    ['', '2025-01-22 13:15:30', 'ja', 'ios', 'テスト事業者D', 202501]
]

# 端末属性マスタデータ
device_attribute_data = [
    ['device_attr', 'description'],
    ['ios', 'iOSデバイス'],
    ['android', 'Androidデバイス'],
    ['windows', 'Windowsデバイス'],
    ['macos', 'macOSデバイス'],
    ['other', 'その他のデバイス']
]

# 利用者属性マスタデータ
user_attribute_data = [
    ['user_attr', 'description'],
    ['ja', '日本語'],
    ['ja-jp', '日本語（日本）'],
    ['ja-kr', '日本語（韓国）'],
    ['ja-gb', '日本語（イギリス）'],
    ['other', 'その他']
]

# ステータスマスタデータ
status_data = [
    ['status', 'description'],
    ['運用中', '正常に稼働中'],
    ['停止中', 'メンテナンスのため停止'],
    ['障害中', '障害発生中'],
    ['準備中', '設置済みで動作確認中'],
    ['撤去予定', '近日中に撤去予定']
]

# CSVファイルの出力（改良版：ダブルクォートありCSV）
with open('./data/input/csv/input_ap.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    for row in ap_data:
        writer.writerow(row)

with open('./data/input/csv/input_connection.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    for row in connection_data:
        writer.writerow(row)

# マスタCSVファイルの出力
with open('./data/master/device_attribute/master_device_attribute.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    for row in device_attribute_data:
        writer.writerow(row)

with open('./data/master/user_attribute/master_user_attribute.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    for row in user_attribute_data:
        writer.writerow(row)

with open('./data/master/status/master_status.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
    for row in status_data:
        writer.writerow(row)

print("英語カラム名版のテストデータ作成が完了しました。")
print("作成したファイル:")
print("- ./data/input/csv/input_ap.csv (英語カラム名)")
print("- ./data/input/csv/input_connection.csv (英語カラム名)")
print("- ./data/master/device_attribute/master_device_attribute.csv (英語カラム名)")
print("- ./data/master/user_attribute/master_user_attribute.csv (英語カラム名)")
print("- ./data/master/status/master_status.csv (英語カラム名)")