"""
2015年のDBから、条件を満たす日毎のツイート数をカウント・ファイルに出力する。
出力ファイル名： <県>_<flag名>_dailycount.txt"
出力フォーマット: [ISO日付  ツイート数]
"""
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from tqdm import tqdm
from pymongo import MongoClient

from s_lib import setup_mongo, setup_mecab


def daterange(start, end):
    for n in range((end - start + timedelta(days=1)).days):
        yield start + timedelta(n)

def count(db, flag):
    # 日毎に、与えられたflagフィールドが1のdocumentをカウントする。
    # 返すリストの要素は"day'\t'month'\t'count"の文字列
    all_day_list = []


    start = datetime(2015, 2, 17, tzinfo=timezone(timedelta(hours=9)))
    end = datetime(2015, 12, 31, tzinfo=timezone(timedelta(hours=9)))

    for date in daterange(start, end):
        today = date.isoformat()
        next_day = (date + timedelta(days=1)).isoformat()

        if flag == "total":
            where = {
                'created_at_iso': {
                    '$gte': today, # ISO形式の文字列と文字列で時間比較してるけどいけてる...？
                    '$lt': next_day
                }
            }
        elif flag == "koyo":
            where = {
                '$or': [ {'icho': 1}, {'kaede': 1}, {'others': 1} ],
                'created_at_iso': {
                    '$gte': today,
                    '$lt': next_day
                }
            }
        else:
            where = {
                flag : 1,
                'created_at_iso': {
                    '$gte': today,
                    '$lt': next_day
                }
            }

        month = str(date.month).zfill(2)
        day = str(date.day).zfill(2)

        col = db['2015-' + month]
        count = col.find(where).count()

        one_day = date.date().isoformat() + '\t' + str(count)
        all_day_list.append(one_day)

    return all_day_list

def main():
    result_dir = '/now24/t.oku/koyo/result/00total_count/'

    db_tk = setup_mongo('2015_tk_twi')
    db_hk = setup_mongo('2015_hk_twi_1208')
    db_is = setup_mongo('2015_is_twi')

    # flags = ["koyo"]# ["total"] # "icho", "kaede", "sonota", "koyo" # koyo
    dbs = [("tk", db_tk), ("hk", db_hk), ("is", db_is)]

    """for pref, db in tqdm(dbs, desc="DB"):
        for flag in tqdm(flags, desc="flag"):
            m_d_count_list = count(db, flag)
            filename = pref + "_" + flag + "_dailycount.tsv"
            os.makedirs(result_dir, exist_ok=True)
            with open(result_dir+filename, "w") as f:
                f.write("\n".join(m_d_count_list))"""

    for pref, db in tqdm(dbs, desc="DB"):
        m_d_count_list = count(db, "total")
        filename = pref + "_" + "total" + "_dailycount.txt"
        os.makedirs(result_dir, exist_ok=True)
        with open(result_dir+filename, "w") as f:
            f.write("\n".join(m_d_count_list))

    print(f"{result_dir} に出力しました。")

main()
