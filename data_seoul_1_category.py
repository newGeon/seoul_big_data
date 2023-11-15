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

from urllib.parse import unquote
from tqdm import tqdm
from bs4 import BeautifulSoup

# from requests_html import HTMLSession
# 서울 열린데이터 광장 데이터 수집 2차
# 분류정보 수집

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

    auth_key = 'aaaaaaaaaaaaaaaaaaaaaaaaaaa'
    auth_key_train = 'bbbbbbbbbbbbbbbbbbbbbbbb'

    # 서울 열린데이터 광장 데이터 & 수집 데이터 타입이 OpenAPI 인 경우에 데이터 수집
    sql = "SELECT ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN " \
          + "  FROM DATA_BASIC_INFO " \
          + " WHERE COLLECT_SITE_ID = 1 "\
          + "   AND CATEGORY_BIG IS NULL "

    cur.execute(sql)
    sql_result = cur.fetchall()

    df_0_data = pd.DataFrame(sql_result).reset_index()
    df_0_data.columns = ['index', 'id', 'collect_site_id', 'data_name', 'data_origin_key', 'collect_data_type',
                         'collect_url_link', 'is_collect_yn']
    df_0_data = df_0_data.drop(columns=['index'], axis=1)

    column_idx = 0

    # 상세 페이지 접속
    for idx, row in df_0_data.iterrows():

        # OpenAPI 만 활용
        data_basic_id = row['id']

        data_type = row['collect_data_type']
        detail_url = row['collect_url_link']

        # 상세 정보 업데이트
        detail_response = requests.get(f'{detail_url}')
        detail_html = detail_response.text
        detail_soup = BeautifulSoup(detail_html, 'html.parser')

        big_category = detail_soup.find("strong", {"class": "side-detail-ctg"})

        print(big_category.contents[0].text.replace("\t","").replace("\n",""))

        # 상세 정보
        detail_info = detail_soup.find("div", {"class": "tbl-base-d align-l only-m2"})
        detail_td_list = detail_info.find_all("td")

        print(detail_td_list[3].text)

        update_sql = " UPDATE DATA_BASIC_INFO SET CATEGORY_BIG = ?, CATEGORY_SMALL = ? WHERE ID = ?"

        update_values = (big_category.contents[0].text.replace("\t","").replace("\n",""), detail_td_list[3].text, data_basic_id)
        cur.execute(update_sql, update_values)

        time.sleep(1)

