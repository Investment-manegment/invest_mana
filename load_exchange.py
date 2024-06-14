from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import pandas as pd
import requests
import logging
import time

from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task(retries=5, retry_delay=timedelta(minutes=1))
def extract(execution_date):
    date = execution_date.strftime('%Y%m%d')
    url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?"
    serviceKey = "authkey=fXeyXDPjhheNmaI9AqmZMX4UvrKqYzya"
    searchdate = "searchdate=" + str(date)
    for i in range(5):  # 5 retries
        try:
            result = requests.get(url + serviceKey + "&" + searchdate + "&" + "data=AP01",verify=False)
            if result.status_code == 200:
                result.encoding = 'utf-8'
                data = result.json()
                logging.info("Extracted data: %s", data)
                return data
            else:
                result.raise_for_status()
        except requests.exceptions.SSLError as e:
            logging.error("SSL error: %s. Retrying...", e)
            time.sleep(5)
            continue
        except requests.exceptions.RequestException as e:
            logging.error("Request error: %s. Retrying...", e)
            time.sleep(5)
            continue

    raise Exception("Failed to fetch data after several retries")

@task()
def transform(data, execution_date):
    records = []
    date = execution_date.strftime('%Y%m%d')
    for item in data:
        code = item.get('result')
        if code == 1:
            records.append({
                'date': date,
                '통화코드': item.get('cur_unit'),
                '전신환(송금) 받으실때': float(item.get('ttb').replace(',', '')),
                '전신환(송금) 보내실때': float(item.get('tts').replace(',', '')),
                '매매 기준율': float(item.get('deal_bas_r').replace(',', '')),
                '장부가격': float(item.get('bkpr').replace(',', '')),
                '년환가료율': float(item.get('yy_efee_r').replace(',', '')),
                '10일환가료율': float(item.get('ten_dd_efee_r').replace(',', '')),
                '서울외국환중개 장부가격': float(item.get('kftc_bkpr').replace(',', '')),
                '서울외국환중개 매매기준율': float(item.get('kftc_deal_bas_r').replace(',', '')),
                '국가통화명': item.get('cur_nm')
            })
    logging.info("Transform ended")
    return records

@task()
def load(records, schema, table):
    cur = get_Redshift_connection() 
    logging.info("Load started")   
    try:
        if records:
            df = pd.DataFrame(records)
            cur.execute("BEGIN;")
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "date" varchar(12),
                "통화코드" VARCHAR(50),
                "전신환(송금) 받으실때" FLOAT,
                "전신환(송금) 보내실때" FLOAT,
                "매매 기준율" FLOAT,
                "장부가격" FLOAT,
                "년환가료율" FLOAT,
                "10일환가료율" FLOAT,
                "서울외국환중개 장부가격" FLOAT,
                "서울외국환중개 매매기준율" FLOAT,
                "국가통화명" VARCHAR(80)
            );
            """
            cur.execute(create_table_query)
            values = [tuple(x) for x in df.to_numpy()]
            insert_query = f"INSERT INTO {schema}.{table} VALUES %s"
            for value in values:
                cur.execute(insert_query, (value,))
            cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error: %s", error)
        cur.execute("ROLLBACK;")  
    logging.info("Load done")

with DAG(
    dag_id='load_exchange_v3',
    start_date=datetime(2024, 1, 1),
    schedule='1 0 * * *',
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(seconds=15)
    }
) as dag:
    
    # Define tasks
    data = extract()
    records = transform(data)
    load_task = load(records, schema='akek1200', table='exchange')

    # Set task dependencies
    data >> records >> load_task

    
# 변경사항 : redshift 계정 / 테이블/ 스키마 / 시작일자 팀원과 맞추기
