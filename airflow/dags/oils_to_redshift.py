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
        return float(soup.find_all(s)[i].text)

    data = [
        ("고급휘발유", "일반상품", "원/리터", change(0, "PRICE"), change(0, "DIFF"), round(change(0, "DIFF") / change(0, "PRICE") * 100, 2)),
        ("휘발유", "일반상품", "원/리터", change(1, "PRICE"), change(1, "DIFF"), round(change(1, "DIFF") / change(1, "PRICE") * 100, 2)),
        ("경유", "일반상품", "원/리터", change(2, "PRICE"), change(2, "DIFF"), round(change(2, "DIFF") / change(2, "PRICE") * 100, 2)),
        ("등유", "일반상품", "원/리터", change(3, "PRICE"), change(3, "DIFF"), round(change(3, "DIFF") / change(3, "PRICE") * 100, 2)),
        ("액화석유가스", "일반상품", "원/리터", change(4, "PRICE"), change(4, "DIFF"), round(change(4, "DIFF") / change(4, "PRICE") * 100, 2))
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

            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s, %s, %s, %s);"
            cur.execute(sql, d)

        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    logging.info("load done")

with DAG(
    dag_id='oils_to_redshift',
    start_date=datetime(2024, 1, 1),
    schedule='0 * * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("akek1200", "oils_price", Variable.get("oils_data_api"))