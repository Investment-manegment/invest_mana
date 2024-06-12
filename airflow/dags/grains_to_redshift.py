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

    url = 'https://finance.naver.com/marketindex/?tabSel=materials#tab_section'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    def change(i):
        return soup.find_all("td")[i].text.replace(",", "").replace("%", "")

    def sign(i):
        if soup.find_all("td")[i + 1].text[0] =="-":
            return -1 * float(soup.find_all("td")[i].text.replace(",", ""))
        else:
            return float(soup.find_all("td")[i].text.replace(",", ""))

    data = []

    for i in range(66, 147, 8):
        data.append((change(i), "일반상품", change(i + 2), float(change(i + 3)), sign(i + 4), float(change(i + 5))))

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
            
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s, %s, %s, %s);"
            cur.execute(sql, d)

        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    logging.info("load done")

with DAG(
    dag_id='grains_to_redshift',
    start_date=datetime(2024, 1, 1),
    schedule='0 * * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("akek1200", "grains_price")