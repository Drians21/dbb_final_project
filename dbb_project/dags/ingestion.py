from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.timezone import make_aware
from datetime import datetime
import pandas as pd
import polars as pl
import os


# Fungsi-fungsi untuk memasukkan data ke PostgreSQL (seperti yang sudah Anda definisikan)

directory = 'data'

def ingest_data_customer_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    
    customers_dataframe = []

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath)
            customers_dataframe.append(df)
    
    df_customers = pd.concat(customers_dataframe, ignore_index=True)
    df_customers.to_sql('customers', engine, if_exists="replace", index=False)

def ingest_data_login_and_coupons_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()

    login_dataframes = []

    for filename in os.listdir(directory):
        if filename == 'coupons.json':
            df_coupons = pd.read_json(os.path.join(directory, filename))
            df_coupons.to_sql('coupons', engine, if_exists="replace", index=False)
        elif filename.startswith('login_attempts'):
            filepath = os.path.join(directory, filename)
            df = pd.read_json(filepath)
            login_dataframes.append(df)
    
    df_login = pd.concat(login_dataframes, ignore_index=True)
    df_login.to_sql('login_attempts', engine, if_exists="replace", index=False)

def ingest_data_order_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    pd.read_parquet("data/order.parquet").to_sql("orders", engine, if_exists="replace", index=False)

def ingest_data_order_item_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    df = pl.read_avro("data/order_item.avro")
    df_pandas = df.to_pandas()
    df_pandas.to_sql("order_items", engine, if_exists="replace", index=False)

def ingest_data_product_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/product.xls").to_sql("products", engine, if_exists="replace", index=False)

def ingest_data_product_category_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/product_category.xls").to_sql("product_category", engine, if_exists="replace", index=False)

def ingest_data_supplier_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    pd.read_excel("data/supplier.xls").to_sql("suppliers", engine, if_exists="replace", index=False)

# Definisikan default_args untuk DAG
default_args = {
    "owner": "kelompok 4",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
}

# Definisikan DAG
with DAG(
    dag_id="ingestion",
    default_args=default_args,
    description="Ingest data to landing zone dan data warehouse postgres",
    schedule_interval="@once",
    start_date=make_aware(datetime.now()),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id='start', dag=dag)
    with TaskGroup('extract_to_landing_zone') as extract_to_landing_zone:
        t1 = PythonOperator(
            task_id="ingest_data_customer_to_postgres",
            python_callable=ingest_data_customer_to_postgres,
            dag=dag,
        )
        t2 = PythonOperator(
            task_id="ingest_data_login_attempts_and_coupons_to_postgres",
            python_callable=ingest_data_login_and_coupons_to_postgres,
            dag=dag,
        )
        t3 = PythonOperator(
            task_id="ingest_data_order_to_postgres",
            python_callable=ingest_data_order_to_postgres,
            dag=dag,
        )
        t4 = PythonOperator(
            task_id="ingest_data_order_item_to_postgres",
            python_callable=ingest_data_order_item_to_postgres,
            dag=dag,
        )
        t5 = PythonOperator(
            task_id="ingest_data_product_to_postgres",
            python_callable=ingest_data_product_to_postgres,
            dag=dag,
        )
        t6 = PythonOperator(
            task_id="ingest_data_product_category_to_postgres",
            python_callable=ingest_data_product_category_to_postgres,
            dag=dag,
        )
        t7 = PythonOperator(
            task_id="ingest_data_supplier_to_postgres",
            python_callable=ingest_data_supplier_to_postgres,
            dag=dag,
        )
        
        [t1, t2, t3, t4, t5, t6, t7]

    end_task = DummyOperator(task_id='end', dag=dag)


    # dependencies
    start_task >> extract_to_landing_zone >> end_task

