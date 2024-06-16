from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

from bs4 import BeautifulSoup
import requests, logging, psycopg2, re, json

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

@task
def scrape_naver_finance(schema, table):
    logging.info("Extract started")
    
    response = requests.get("https://finance.naver.com/world/", headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    def extracted_json_str(data):
        start_index = data.find("{", data.find("jindo.$H(") + len("jindo.$H("))
        end_index = data.rfind("}", 0, data.rfind(");")) + 1 
        json_str = data[start_index:end_index]
        return json_str

    def extract_js_variable_value(var_name):
        script_tags = soup.find_all('script', string=re.compile(var_name))
        for tag in script_tags:
            match = re.search(fr'{var_name} = (.*?);', tag.string, re.DOTALL)
            if match:
                return match.group(1)
        return None

    dict_data = []
    var_list = ["americaData", "asiaData", "europeAfricaData"]

    for var_name in var_list:
        stock_data = extract_js_variable_value(var_name)
        logging.info(f"stock_data: {stock_data}")
        data_str = extracted_json_str(stock_data)
        data_dict = json.loads(data_str)

        for key, value in data_dict.items():
            dict_data.append({
                "국가명": value['natcKnam'],
                "지수명": value['knam'],
                "현재가": float(value['monthCloseVal']),
                "전일대비": float(value['diff']),
                "등락률": float(value['rate'])
            })

    logging.info("Transform ended")
    logging.info("Load started")
    
    return dict_data  # 수집된 데이터 반환

@task(task_id="load_data_to_redshift")
def load_to_redshift(dict_data: list, schema, table):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")

        # 기존 데이터 삭제
        delete_sql = f"DELETE FROM {schema}.{table};"
        cur.execute(delete_sql)

        # executemany를 사용하여 데이터 삽입
        sql = f"INSERT INTO {schema}.{table} (국가명, 지수명, 현재가, 전일대비, 등락률) VALUES (%s, %s, %s, %s, %s);"
        tuple_data = [(item['국가명'], item['지수명'], item['현재가'], item['전일대비'], item['등락률']) for item in dict_data]
        psycopg2.extras.execute_batch(cur, sql, tuple_data, page_size=1000)

        cur.execute("COMMIT;")
        logging.info("Load to Redshift completed successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(f"Error loading data to Redshift: {error}")
        cur.execute("ROLLBACK;")
        raise  # 에러 발생 시 Task 실패 처리

with DAG(
    dag_id='overseas_stock_to_redshift',
    start_date=datetime(2024, 1, 1),
    schedule='0 * * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    overseas_data = scrape_naver_finance("akek1200", "overseas_data") # 스키마와 테이블 이름 변수로 지정
    load_to_redshift(overseas_data, "akek1200", "overseas_data")