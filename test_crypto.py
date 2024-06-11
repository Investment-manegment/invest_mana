import requests
import json
import logging
from collections import OrderedDict
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table):
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    data = response.json()
    ticker_data = data['data']
    ticker_data.pop('date', None)
    top_10 = sorted(ticker_data.items(), key=lambda item: float(item[1]['max_price']), reverse=True)[:10]
    sorted_ticker_data = OrderedDict(top_10)
    sorted_json_data = json.dumps(sorted_ticker_data, indent=4, ensure_ascii=False)
    sorted_ticker_data = json.loads(sorted_json_data)
    ret = []
    for k, v in sorted_ticker_data.item():
        ret.append("('{}',{},{})".format(k, v["max_price"], v["min_price"]))

    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    unit varchar,
    min_price BIGINT,
    max_price BIGINT,
    created_date timestamp default GETDATE()
);
"""
    insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(ret)
    logging.info(drop_recreate_sql)
    logging.info(insert_sql)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

with DAG(
    dag_id = 'crypto_to_Redshift',
    start_date = datetime(2023,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 0 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("akek1200", "crypto")