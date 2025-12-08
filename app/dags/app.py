from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
from datetime import datetime, timedelta

def extract_currency_data(ti):
    data = {}
    response = requests.get("https://free.ratesdb.com/v1/rates", params={"from": "INR"})
    if response.status_code == 200:
        data = response.json()["data"]
        print(data)
        df = pd.DataFrame(data['rates'].items(), columns=['currency_code', 'rate'])
        df["date"] = data["date"]
        df["inverse_rate"] = 1/df["rate"]
        df = df[["date", "currency_code", "rate", "inverse_rate"]]
        ti.xcom_push(key='currency_data', value=df.to_dict('records'))

def insert_master_fixtures():
    master_currency_df = pd.read_excel("/opt/airflow/dags/master_currency.xlsx", sheet_name="master_currency_country")
    master_currency_data = master_currency_df.to_dict('records')
    postgres_hook = PostgresHook(postgres_conn_id='currency_connection')
    insert_query = """
    INSERT INTO master_currency (currency_code, currency_name, country, continent, region, flag_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    for master_currency in master_currency_data:
        postgres_hook.run(insert_query, parameters=(master_currency['currency_code'], master_currency['currency_name'], master_currency['country'], master_currency['continent'], master_currency['region'], master_currency['flag_url']))


def insert_currency_data_into_postgres(ti):
    currency_data = ti.xcom_pull(key='currency_data', task_ids='fetch_currency_data')
    if not currency_data:
        raise ValueError("No currency data found")

    postgres_hook = PostgresHook(postgres_conn_id='currency_connection')

    check_query = "SELECT COUNT(*) FROM inr_currency WHERE date = %s;"
    count = postgres_hook.get_first(check_query, parameters=(currency_data[0]['date'],))[0]

    if count > 0:
        print(f"Data for {currency_data[0]['date']} already exists. Skipping insert.")
        return
    insert_query = """
    INSERT INTO inr_currency (date, currency_code, rate, inverse_rate)
    VALUES (%s, %s, %s, %s);
    """

    
    for currency in currency_data:
        postgres_hook.run(insert_query, parameters=(currency['date'], currency['currency_code'], currency['rate'], currency['inverse_rate']))

def check_master_table_exists():
    hook = PostgresHook(postgres_conn_id="currency_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'master_currency'
        );
    """)

    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return "skip_create_table" if exists else "create_master_table"
    



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_and_load_currency_data',
    default_args=default_args,
    schedule='@daily',
)

task_create_master_table = PostgresOperator(
    task_id='create_master_table',
    postgres_conn_id='currency_connection',
    sql="""
    CREATE TABLE master_currency (
        id SERIAL PRIMARY KEY,
        currency_code VARCHAR(10) NOT NULL,
        currency_name TEXT,
        country TEXT,
        continent TEXT,
        region TEXT,
        flag_url TEXT
    );
    """,
    dag=dag
)

check_master_table = BranchPythonOperator(
    task_id="check_master_table",
    python_callable=check_master_table_exists,
    dag=dag
)

task_skip_create_table = EmptyOperator(
    task_id="skip_create_table",
    dag=dag
)


task_insert_master_data = PythonOperator(
    task_id='insert_master_data',
    python_callable=insert_master_fixtures,
    # trigger_rule='all_success',
    dag=dag,
)


task_fetch_currency_data = PythonOperator(
    task_id='fetch_currency_data',
    python_callable=extract_currency_data,
    dag=dag,
)

task_create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='currency_connection',
    sql = """
    CREATE TABLE IF NOT EXISTS inr_currency (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        currency_code VARCHAR(10) NOT NULL,
        rate NUMERIC(15,6),
        inverse_rate NUMERIC(15,6),
        timestamp TIMESTAMP DEFAULT NOW(),

        CONSTRAINT fk_currency_code
        FOREIGN KEY (currency_code)
        REFERENCES master_currency(currency_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
    );
    """
    ,
    dag=dag,
)

task_insert_currency_data = PythonOperator(
    task_id='insert_currency_data',
    python_callable=insert_currency_data_into_postgres,
    dag=dag,
)

check_master_table >> [task_create_master_table, task_skip_create_table]
task_create_master_table >> task_insert_master_data
task_fetch_currency_data >> task_create_table >> task_insert_currency_data