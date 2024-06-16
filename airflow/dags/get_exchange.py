from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn


def upload_df_to_redshift(schema, table, execution_date):
    date = execution_date.strftime('%Y%m%d')
    print(f"Execution date: {execution_date}")
    print(f"Formatted date: {date}")

    url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?"
    serviceKey = "authkey=fXeyXDPjhheNmaI9AqmZMX4UvrKqYzya"
    searchdate = "searchdate=" + str(date)

    result = requests.get(url + serviceKey + "&" + searchdate + "&" + "data=AP01")
    if result.status_code == 200:
        result.encoding = 'utf-8'
        data = result.json()

        row = []
        for item in data:
            code = item.get('result')
            if code == 1:
                row.append({
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

        if row:
            df = pd.DataFrame(row)
            conn = get_Redshift_connection()
            cur = conn.cursor()

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
            conn.commit()

            values = [tuple(x) for x in df.to_numpy()]
            insert_query = f"INSERT INTO {schema}.{table} VALUES %s"
            # extras.execute_values(cur, insert_query, values)
            for i in values:
                cur.execute(insert_query, (i,))
            conn.commit()

            cur.close()
            conn.close()

            print("Data upload completed.")
        else:
            print("No data found.")
    else:
        print(f"Failed to retrieve data: {result.status_code}")

with DAG(
    dag_id='upload_exchange',
    start_date = datetime(2024, 6, 1),
    schedule='1 0 * * *',
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(seconds=20),
        "donot_pickle": True
    }
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_redshift',
        python_callable=upload_df_to_redshift,
        op_kwargs={'schema' : 'wsh120',
                   'table' : 'exchange',},
        provide_context=True

    )

upload_task


