from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import requests, re, time
from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def create_top10(schema):
    cur = get_Redshift_connection()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.stock_top10 (
        rank VARCHAR(3),
        title VARCHAR(255),
        code VARCHAR(10),
        price BIGINT,  
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""")
    cur.connection.commit()

def create_stock_history(schema):
    cur = get_Redshift_connection()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.stock_history (
        code VARCHAR(10),
        date DATE,
        diff BIGINT,
        closing_price BIGINT,
        opening_price BIGINT,
        high_price BIGINT,
        low_price BIGINT,  
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""")
    cur.connection.commit()

def crawl_rank_to_naver_stock():
    url = "https://finance.naver.com/sise/sise_quant.naver"
    response = requests.get(url)
    response.encoding = 'euc-kr'

    cur = get_Redshift_connection()
    schema = Variable.get("schema")

    soup = BeautifulSoup(response.text, 'html.parser')
    top10_stocks = soup.select('table.type_2 > tr')
    rank = 0
    stocks = []
    create_top10(schema)
    cur.execute("BEGIN;")
    cur.execute(f"CREATE TEMP TABLE t_stock_top10 AS SELECT * FROM {schema}.stock_top10;")
    for stock in top10_stocks:
        title = stock.select_one('td:nth-child(2) > a')
        if title:
            rank += 1
            title = title.get_text(strip=True)
            code = stock.select_one('td:nth-child(2) > a')['href'].split("=")[1]
            price = stock.select_one('td:nth-child(3)').get_text(strip=True).replace(",", "")
            volume = stock.select_one('td:nth-child(6)').get_text(strip=True).replace(",","")
            stocks.append({'rank': rank, 'title': title, 'volume': volume, 'price': price, 'code': code})
            sql = f"""
                INSERT INTO t_stock_top10 (rank, title, code, price, volume, created_at) VALUES ({rank}, '{title}', '{code}', {price}, {volume}, getdate());
            """
            cur.execute(sql)
        if rank == 10:
            break
    cur.execute(f"DROP TABLE IF EXISTS {schema}.stock_top10;")
    sql = f"""
    CREATE TABLE {schema}.stock_top10 AS 
    SELECT rank, title, code, price, volume, created_at FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code,LEFT(created_at,10) ORDER BY created_at DESC) seq 
        FROM t_stock_top10
    ) WHERE seq = 1;
    """
    cur.execute(sql)
    print(stocks)
    for stock in stocks :
        crawl_stock_history(stock['code'])
        # crawl_stock_history('005930')
        time.sleep(2.5)
    cur.execute("COMMIT;")

def crawl_stock_history(code):
    url = f"https://finance.naver.com/item/sise_day.naver?code={code}"
    schema = Variable.get("schema")
    headers = {'User-Agent': 'Mozilla/5.0'}

    response = requests.get(url, headers=headers)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')

    cur = get_Redshift_connection()

    table = soup.find('table', class_='type2')
    rows = table.find_all('tr')
    chart_data = []
    create_stock_history(schema)
    cur.execute("BEGIN;")
    print("!!!!!!!!!!!! BEGIN",code)
    cur.execute(f"CREATE TEMP TABLE t_stock_history AS SELECT * FROM {schema}.stock_history;")
    for row in rows[2:]:
        cols = row.find_all('td')
        if len(cols) < 7:
            continue
        date = cols[0].get_text(strip=True)
        closing_price = cols[1].get_text(strip=True).replace(',', '')
        diff = re.sub(r'상승|하락|보합|상한가|하한가,', lambda x: {
            '상승': '',
            '상한가': '',
            '하락': '-',
            '하한가': '-',
            '보합': '0',
        }[x.group()], cols[2].get_text(strip=True)).replace(',', '')
        opening_price = cols[3].get_text(strip=True).replace(',', '')
        high_price = cols[4].get_text(strip=True).replace(',', '')
        low_price = cols[5].get_text(strip=True).replace(',', '')
        volume = cols[6].get_text(strip=True).replace(',', '')

        sql = f"""
            INSERT INTO t_stock_history (code, "date", diff, closing_price, opening_price, high_price, low_price, volume, created_at) 
            VALUES ('{code}', '{date}', {diff}, {closing_price}, {opening_price}, {high_price}, {low_price}, {volume}, getdate());
        """
        # print("!!!!!!!!!!!!",sql)
        cur.execute(sql)
        print(sql)
        chart_data.append({
            'code': code,
            'date': date,
            'diff': diff,
            'closing_price': closing_price,
            'opening_price': opening_price,
            'high_price': high_price,
            'low_price': low_price,
            'volume': volume
        })
        if len(chart_data) >= 7:
            break

    cur.execute(f"DROP TABLE IF EXISTS {schema}.stock_history;")
    print("!!!!!!!!!!!! DROP")
    sql = f"""
        CREATE TABLE {schema}.stock_history AS 
        SELECT code, "date", diff, closing_price, opening_price, high_price, low_price, volume, created_at FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code,date ORDER BY created_at DESC) seq 
        FROM t_stock_history
        ) WHERE seq = 1;
    """
    
    cur.execute(sql)
    print("!!!!!!!!!!!!END")
    cur.execute("COMMIT;")
    print(chart_data)

with DAG(
    dag_id='naver_stock_to_redshift',
    start_date=datetime(2024, 6, 11),
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:
    get_rank = PythonOperator(
        task_id='crawl_rank_to_naver_stock',
        python_callable=crawl_rank_to_naver_stock,
    )
    get_rank