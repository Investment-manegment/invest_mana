from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

from bs4 import BeautifulSoup
import requests
import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table):
    logging.info("Extract started")

    url = "https://krjx.co.kr/"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    def change(i):
        return int(soup.find_all("span")[i].text.replace(",", "").strip())

    def updown(i):
        if "#FF0000" in str(soup.find_all("span")[i]):
            return int(soup.find_all("span")[i].text.replace(",", "").strip())
        elif "0098FF" in str(soup.find_all("span")[i]):
            return -1 * int(soup.find_all("span")[i].text.replace(",", "").strip())
        else:
            return 0

    data = [
        ["금", change(33), updown(34), round(updown(34) / change(33) * 100, 2)],
        ["은", change(35), updown(36), round(updown(36) / change(35) * 100, 2)],
        ["백금", change(37), updown(38), round(updown(38) / change(37) * 100, 2)],
        ["팔라듐", change(39), updown(40), round(updown(40) / change(39) * 100, 2)]
    ]

    print("=============")
    print(data)

    logging.info("Transform ended")
    logging.info("Load started")

    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")

        for d in data:
            print("=============")
            print(d)
            
            metal_type = d[0]
            metal_price = d[1]
            diff_before = d[2]
            ratio_before = d[3]

            sql = f"INSERT INTO {schema}.{table} VALUES ('{metal_type}', '{metal_price}', '{diff_before}', '{ratio_before}')"
            cur.execute(sql)

        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    logging.info("load done")

with DAG(
    dag_id='metals_to_redshift',
    start_date=datetime(2024, 1, 1),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 * * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("akek1200", "metals_price")