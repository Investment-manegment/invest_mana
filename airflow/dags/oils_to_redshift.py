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
def etl(schema, table, code):
    logging.info("Extract started")

    url = f"https://www.opinet.co.kr/api/avgAllPrice.do?out=xml&code={code}"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'lxml-xml')

    def change(i, s):
        return soup.find_all(s)[i].text

    data = [
        [change(0, "PRODNM"), float(change(0, "PRICE")), float(change(0, "DIFF")), round(float(change(0, "DIFF")) / float(change(0, "PRICE")) * 100, 2)],
        [change(1, "PRODNM"), float(change(1, "PRICE")), float(change(1, "DIFF")), round(float(change(1, "DIFF")) / float(change(1, "PRICE")) * 100, 2)],
        [change(2, "PRODNM"), float(change(2, "PRICE")), float(change(2, "DIFF")), round(float(change(2, "DIFF")) / float(change(2, "PRICE")) * 100, 2)],
        [change(3, "PRODNM"), float(change(3, "PRICE")), float(change(3, "DIFF")), round(float(change(3, "DIFF")) / float(change(3, "PRICE")) * 100, 2)],
        [change(4, "PRODNM"), float(change(4, "PRICE")), float(change(4, "DIFF")), round(float(change(4, "DIFF")) / float(change(4, "PRICE")) * 100, 2)]
    ]

    logging.info("Transform ended")
    logging.info("Load started")

    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")

        for d in data:
            oil_type = d[0]
            oil_price = d[1]
            diff_before = d[2]
            ratio_before = d[3]

            sql = f"INSERT INTO {schema}.{table} VALUES ('{oil_type}', '{oil_price}', '{diff_before}', '{ratio_before}')"
            cur.execute(sql)

        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    logging.info("load done")

with DAG(
    dag_id='oils_to_redshift',
    start_date=datetime(2024, 1, 1),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 * * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("akek1200", "oils_price", Variable.get("oils_data_api"))