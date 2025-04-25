#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import sys

def convert_csv_with_quotes(input_file, output_file):
    """CSVファイルの各フィールドをダブルクォートで囲む"""
    with open(input_file, 'r', encoding='utf-8') as f_in:
        reader = csv.reader(f_in)
        with open(output_file, 'w', encoding='utf-8') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_ALL)
            for row in reader:
                writer.writerow(row)
    print(f"{input_file} -> {output_file} に変換しました")

# メイン処理
for csv_file in os.listdir('./data/input/csv'):
    if csv_file.endswith('.csv'):
        input_path = os.path.join('./data/input/csv', csv_file)
        output_path = os.path.join('./data/input/csv', f"{csv_file}.quoted")
        convert_csv_with_quotes(input_path, output_path)
        # 元のファイルを置き換え
        os.rename(output_path, input_path)
        print(f"{csv_file} を修正しました")

print("すべてのCSVファイルを変換しました")
