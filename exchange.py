from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras

def upload_df_to_redshift():
    url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?"
    serviceKey = "authkey=fXeyXDPjhheNmaI9AqmZMX4UvrKqYzya"

    # 날짜 YYYYMMDD
    date = 20240605

    searchdate = "searchdate=" + str(date)

    # parsing 하기
    result = requests.get(url + serviceKey + "&" + searchdate + "&" + "data=AP01")
    if result.status_code == 200:
        result.encoding = 'utf-8'
        data = result.json()

        row = []
        for item in data:
            code = item.get('result')
            if code == 1:
                row.append({
                    'date' : date,
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

        if not row:
            return  # 데이터가 없으면 함수 종료

        df = pd.DataFrame(row)

        # Redshift 연결 정보
        dbname = 'dev'
        user = 'wsh120'
        password = 'Wsh120!1'
        host = 'learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com'
        port = '5439'
        schema = 'wsh120'
        table = 'exchange'

        # Redshift에 연결
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        cur = conn.cursor()

        # 테이블이 존재하지 않으면 생성
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

        # DataFrame을 레드쉬프트에 업로드
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = f"INSERT INTO {schema}.{table} VALUES %s"
        extras.execute_values(cur, insert_query, values)
        conn.commit()

        # 연결 종료
        cur.close()
        conn.close()

    else:
        print(f"Failed to retrieve data: {result.status_code}")

with DAG(
    dag_id='mydag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 27),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_redshift',
        python_callable=upload_df_to_redshift,
        start_date=dag.default_args['start_date']
    )

upload_task
