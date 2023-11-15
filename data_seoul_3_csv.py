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
# 세부 항목 수집 및 OpenAPI 접속 후 데이터 수집

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
    
    auth_key = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    auth_key_train = 'bbbbbbbbbbbbbbbbbbbbbbbbbb'

    # 서울 열린데이터 광장 데이터 & 수집 데이터 타입이 OpenAPI 인 경우에 데이터 수집
    sql = "SELECT ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN "\
        + "  FROM DATA_BASIC_INFO "\
        + " WHERE COLLECT_SITE_ID = 1 "\
        + "   AND IS_COLLECT_YN = 'Y' "\
        + "   AND ID = '23' "

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

        # 상세 정보 업데이트
        detail_response = requests.get(f'{detail_url}')
        detail_html = detail_response.text
        detail_soup = BeautifulSoup(detail_html, 'html.parser')

        # 상세 정보
        detail_info = detail_soup.find("div", {"class": "tbl-base-d align-l only-d2"})
        detail_td_list = detail_info.find_all("td")

        # OPEN API 샘플 정보
        openapi_url = basic_url + 'openApiView.do?infId=' + row['data_origin_key'] + '&srvType=A'

        print(openapi_url)

        openapi_response = requests.get(f'{openapi_url}')
        openapi_html = openapi_response.text
        openapi_soup = BeautifulSoup(openapi_html, 'html.parser')

        basic_openapi_info = openapi_soup.find_all("div", {"class": "tbl-base-s"})

        # Sample OpenAPI URL 
        sample_openapi_url = basic_openapi_info[0].find("a", href=True)['href']

        print(sample_openapi_url)

        table_info_list = sample_openapi_url.split('/sample/xml/')[1]
        if data_basic_id == 239:
            master_openapi_url = sample_openapi_url.replace("/sample/", "/" + auth_key_train + "/")
        else:
            master_openapi_url = sample_openapi_url.replace("/sample/", "/" + auth_key + "/")
        if master_openapi_url.strip()[-1] != "/":
            master_openapi_url = master_openapi_url + "/"

        if data_basic_id == 239:
            master_openapi_url = master_openapi_url.rsplit('/', 1)[0]
        elif data_basic_id == 240:
            master_openapi_url = master_openapi_url.rsplit('/', 2)[0]
        else:
            master_openapi_url = master_openapi_url.rsplit('/', 3)[0]

        # 데이터 물리 저장 테이블 정보
        # 논리 테이블 영어
        table_logical_name = table_info_list.split('/')[0]
        table_logical_name = re.sub(r'(?<!^)(?=[A-Z])', '_', table_logical_name).upper()

        table_physical_name = "NLDATA_" + str(data_basic_id).rjust(6, "0")
        table_orig_name = "TMP_" + str(data_basic_id).rjust(6, "0")

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        select_table_sql = """SELECT ID, START_IDX FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID = {} ORDER BY ID DESC""".format(data_basic_id)
        
        cur.execute(select_table_sql)
        manage_table_result = cur.fetchall()
        manage_table_id = manage_table_result[0][0]
        start_idx = manage_table_result[0][1]

        # 출력값 coulmn_info
        data_output_list = basic_openapi_info[2].find_all("td")
        
        real_data_column = "(ID, "

        for idx in range(0, len(data_output_list)):
            if idx % 3 == 0:
                if data_output_list[idx].text != "공통":
                    # print(str(idx+1) + " : " + data_output_list[idx].text + " : " + data_output_list[idx+1].text + " : " + data_output_list[idx+2].text)
                    logical_column_hangule = data_output_list[idx+2].text
                    logical_column_english = data_output_list[idx+1].text
                    physical_column_order = int(data_output_list[idx].text)
                    physical_column_name = "COL_" + str(physical_column_order).rjust(3, "0")
                   
                    ts_column = time.time()
                    timestamp_column = datetime.datetime.fromtimestamp(ts_column).strftime('%Y-%m-%d %H:%M:%S')

                    real_data_column += physical_column_name + ", "

                    column_idx += 1

        time.sleep(2)

        # DATA INSERT 컬럼 모음
        real_data_column = real_data_column.strip()
        real_data_column = real_data_column[:-1]
        real_data_column = real_data_column + ")"
       
        # OpenAPI 
        print("-------------------------------------------------------------------")
        print(sample_openapi_url)
        print(master_openapi_url)

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
