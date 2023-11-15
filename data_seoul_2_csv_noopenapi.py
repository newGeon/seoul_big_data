import os
from random import sample
import re
import time
import json
import pandas as pd
from pymysql import NULL
import requests
import datetime
import jaydebeapi
import xml.etree.ElementTree as ET
import xml.etree.cElementTree as etree
import csv

from urllib.parse import unquote
from tqdm import tqdm
from bs4 import BeautifulSoup

# from requests_html import HTMLSession
# 서울 열린데이터 광장 데이터 수집 2차
# OpenAPI 없이 CSV 로만 처리할때

if __name__=="__main__":

    print('==== Tibero DB Connection ===============================================================')

    # Tibero DB Connection
    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@127.0.0.1:8629:tibero",
        ["root", "1234"],
        "tibero6-jdbc.jar",
    )
    cur = conn.cursor()

    basic_url = "http://data.seoul.go.kr/dataList/"

    auth_key = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    auth_key_train = 'bbbbbbbbbbbbbbbbbbbbbbbb'

    # 서울 열린데이터 광장 데이터 & 수집 데이터 타입이 OpenAPI 인 경우에 데이터 수집
    sql = "SELECT ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN "\
        + "  FROM DATA_BASIC_INFO "\
        + " WHERE COLLECT_SITE_ID = 1 "\
        + "   AND IS_COLLECT_YN = 'Y' "\
        + "   AND ID IN (5758) "

    cur.execute(sql)
    sql_result = cur.fetchall()

    df_0_data = pd.DataFrame(sql_result).reset_index()
    df_0_data.columns = ['index', 'id', 'collect_site_id', 'data_name', 'data_origin_key', 'collect_data_type', 'collect_url_link', 'is_collect_yn']
    df_0_data = df_0_data.drop(columns=['index'], axis=1)

    column_idx = 0

    # 상세 페이지 접속
    for idx, row in df_0_data.iterrows():
        
        # OpenAPI 만 활용
        data_basic_id = row['id']

        data_type = row['collect_data_type']
        detail_url = row['collect_url_link']

        print(detail_url)

        table_physical_name = "NLDATA_" + str(data_basic_id).rjust(6, "0")
        table_orig_name = "TMP_" + str(data_basic_id).rjust(6, "0")

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        select_table_sql = """SELECT ID, START_IDX FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID = {} ORDER BY ID DESC""".format(data_basic_id)
        
        cur.execute(select_table_sql)
        manage_table_result = cur.fetchall()
        manage_table_id = manage_table_result[0][0]
        start_idx = manage_table_result[0][1]

        save_xml_path = './data/seoul'

        list_total_count = 0
        row = 0

        f = open(save_xml_path + "/" + table_orig_name + '.csv', 'r')
        rdr = csv.reader(f)

        sql = "SELECT ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, PHYSICAL_COLUMN_NAME, PHYSICAL_COLUMN_TYPE " \
              + "  FROM MANAGE_PHYSICAL_COLUMN " \
              + " WHERE DATA_PHYSICAL_ID = '" + str(manage_table_id) + "' " \
              + " ORDER BY PHYSICAL_COLUMN_ORDER "

        print(sql)
        cur.execute(sql)
        sql_result = cur.fetchall()

        df_1_data = pd.DataFrame(sql_result).reset_index()
        df_1_data.columns = ['index', 'id', 'data_physical_id', 'logical_column_korean', 'physical_column_name',
                             'physical_column_type']
        df_1_data = df_1_data.drop(columns=['index'], axis=1)

        real_data_column = "(ID, "

        for idx2, row2 in df_1_data.iterrows():
            real_data_column += row2["physical_column_name"] + ", "

        real_data_column = real_data_column.strip()
        real_data_column = real_data_column[:-1]
        real_data_column = real_data_column + ")"

        list_total_count = start_idx
        for line in rdr:
            if row != 0:
                if start_idx == 0 or (start_idx != 0 and start_idx <= row):
                    insert_data_values = [row]
                    for idx2, row2 in df_1_data.iterrows():
                        insert_data_values.append(line[idx2])

                    insert_data_values = tuple(insert_data_values)

                    insert_data_sql = "INSERT INTO " + table_orig_name + " " + real_data_column + " VALUES " + str(insert_data_values)

                    # print(insert_data_sql)
                    cur.execute(insert_data_sql)

                    list_total_count += 1
            row += 1

        print("-----------------------------------------------------------------------------------------")    
        print("-----------------------------------------------------------------------------------------")

        update_insert_sql = """UPDATE MANAGE_PHYSICAL_TABLE SET DATA_INSERTED_YN = 'Y' """ \
                            """ , DATA_INSERT_DATE = SYSDATE """ \
                            """ , DATA_INSERT_ROW = ? """ \
                            """WHERE ID = ? """

        update_insert_values = (list_total_count, manage_table_id)

        cur.execute(update_insert_sql, update_insert_values)

    print('=========================================================================================')    
    print('=========================================================================================')
