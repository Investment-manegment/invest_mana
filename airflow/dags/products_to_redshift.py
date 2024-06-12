from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def load(schema, table):
    logging.info("Load started")

    sql = """
    INSERT INTO akek1200.products_price
    SELECT * FROM akek1200.oils_price
    UNION
    SELECT * FROM akek1200.metals_price
    UNION
    SELECT * FROM akek1200.grains_price;"""

    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
        cur.execute(sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    logging.info("Load done")

with DAG(
    dag_id='products_to_redshift',
    start_date=datetime(2024, 1, 1),
    schedule='5 * * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    load("akek1200", "products_price")