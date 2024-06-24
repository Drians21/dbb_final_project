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


def fact_sales():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()

    query_fact_sales = """
        SELECT 
            c."id" as customer_id, 
            o."id" as order_id, 
            s."id" as supplier_id, 
            l."id" as login_attempts_id, 
            p."id" as product_id,
            o."created_at" as order_date,
            oi."amount" as amount,
            p."price" as price
        from "orders" o
        join "customers" c on o."customer_id" = c."id"
        join "login_attempts" l on c."id" = l."customer_id"
        join "order_items" oi on o."id" = oi.order_id
        join "products" p on oi."product_id" = p."id"
        join "suppliers" s on p."supplier_id" = s."id"
        """
    
    df_fact_sales = hook.get_pandas_df(sql=query_fact_sales)
    df_fact_sales.dropna(inplace=True)
    df_fact_sales.drop_duplicates(inplace=True)
    df_fact_sales.to_sql('fact_sales', engine2, if_exists="replace", index=False)


def dim_customers():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()

    query_customers = """
        SELECT * FROM "customers"
    """

    df_dim_customers = hook.get_pandas_df(sql=query_customers)
    df_dim_customers.drop(columns='Unnamed: 0', inplace=True)
    df_dim_customers.dropna(inplace=True)
    df_dim_customers.drop_duplicates(inplace=True)
    df_dim_customers.to_sql('dim_customers', engine2, if_exists="replace", index=False)


def dim_products():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()

    query_products = """
        SELECT 
            p."id" as product_id, 
            p."name" as product_name, 
            c."name" as product_category 
            FROM "products" p
            JOIN "product_category" c ON p."category_id" = c."id"
    """

    df_dim_products = hook.get_pandas_df(sql=query_products)
    df_dim_products.dropna(inplace=True)
    df_dim_products.drop_duplicates(inplace=True)
    df_dim_products.to_sql('dim_products', engine2, if_exists="replace", index=False)


def dim_suppliers():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()

    query_suppliers = """
        SELECT
            "id" as supplier_id,
            "name" as supplier_name,
            "country" as supplier_country
        FROM "suppliers"
    """

    df_dim_suppliers = hook.get_pandas_df(sql=query_suppliers)
    df_dim_suppliers.dropna(inplace=True)
    df_dim_suppliers.drop_duplicates(inplace=True)
    df_dim_suppliers.to_sql('dim_suppliers', engine2, if_exists="replace", index=False)


def dim_login_attempts():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()


    query_login_attempts = """
        SELECT
            "id" as login_attempts_id,
            "login_successful",
            "attempted_at"
        FROM "login_attempts"
    """

    df_dim_login_attempts = hook.get_pandas_df(sql=query_login_attempts)
    df_dim_login_attempts.dropna(inplace=True)
    df_dim_login_attempts.drop_duplicates(inplace=True)
    df_dim_login_attempts.to_sql('dim_login_attempts', engine2, if_exists="replace", index=False)


def dim_orders():
    hook = PostgresHook(postgres_conn_id="postgres_dl")
    engine = hook.get_sqlalchemy_engine()
    hook2 = PostgresHook(postgres_conn_id="postgres_dw")
    engine2 = hook2.get_sqlalchemy_engine()

    query_orders = """
        SELECT
            o."id" as order_id,
            o."status" as order_status,
            cp."discount_percent" as discount_percent
        FROM "orders" o
        JOIN "order_items" oi ON o."id" = oi."order_id"
        JOIN "coupons" cp ON oi."coupon_id" = cp."id"
    """

    df_dim_orders = hook.get_pandas_df(sql=query_orders)
    df_dim_orders.dropna(inplace=True)
    df_dim_orders.drop_duplicates(inplace=True)
    df_dim_orders.to_sql('dim_orders', engine2, if_exists="replace", index=False)




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
    dag_id="tranformation_and_load_to_dw",
    default_args=default_args,
    description="tranformation_and_load_to_dw",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id='start', dag=dag)
    with TaskGroup('tranformation_and_load_to_dw') as tranformation_and_load_to_dw:
        fact_sales = PythonOperator(
            task_id="fact_sales",
            python_callable=fact_sales,
            dag=dag,
        )
        dim_customers = PythonOperator(
            task_id="dim_customers",
            python_callable=dim_customers,
            dag=dag,
        )
        dim_products = PythonOperator(
            task_id="dim_products",
            python_callable=dim_products,
            dag=dag,
        )
        dim_suppliers = PythonOperator(
            task_id="dim_suppliers",
            python_callable=dim_suppliers,
            dag=dag,
        )
        dim_login_attempts = PythonOperator(
            task_id="dim_login_attempts",
            python_callable=dim_login_attempts,
            dag=dag,
        )
        dim_orders = PythonOperator(
            task_id="dim_orders",
            python_callable=dim_orders,
            dag=dag,
        )

        [fact_sales, dim_customers, dim_products, dim_suppliers, dim_login_attempts, dim_orders]

    end_task = DummyOperator(task_id='end', dag=dag)
    start_task >> tranformation_and_load_to_dw >> end_task