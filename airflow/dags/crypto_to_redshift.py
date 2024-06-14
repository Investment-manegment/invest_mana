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
    data_trade_val = sorted(ticker_data.items(), key=lambda item: float(item[1]['acc_trade_value_24H']), reverse=True)[:20]
    sorted_data_trade_val = OrderedDict(data_trade_val)
    dumps_data_trade_val = json.dumps(sorted_data_trade_val, indent=4, ensure_ascii=False)
    json_data_trade_val = json.loads(dumps_data_trade_val)
    trade_val = []
    for k, v in json_data_trade_val.items():
        trade_val.append("('{}',{},{},{},{},{},{},{},{})".format(k, v["max_price"], v["min_price"], v["units_traded"], v["acc_trade_value"], v["units_traded_24H"], v["acc_trade_value_24H"], v["fluctate_24H"], v["fluctate_rate_24H"]))

    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    unit varchar(10),
    max_price FLOAT,
    min_price FLOAT,
    units_traded FLOAT,
    trade_value FLOAT,
    units_traded_24H FLOAT,
    acc_trade_value_24H FLOAT,
    fluctate_24H FLOAT,
    fluctate_rate_24H FLOAT
);
"""
    insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(trade_val)
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
    dag_id = 'crypto_to_redshift',
    start_date = datetime(2023,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 * * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
) as dag:
    etl("akek1200", "crypto_trade_value")
