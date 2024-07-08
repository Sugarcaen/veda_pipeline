from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_faker import generate_data
from db_utils import push_purchases_to_graph, ingest_redshift_data, push_roasting_profile_to_graph

default_args = {
    "owner": "airflow"
}

USER_PATH = '/users.csv'
RETAIL_PRODUCTS_PATH = '/retail_products.csv'
TRANSACTIONS_PATH = '/transactions.csv'

with DAG(
    dag_id="generate_users_and_transactions",
    default_args=default_args,
    schedule_interval="@daily"
) as dag:

    generate_data_task=PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        op_kwargs={"target": "s3", "user_path":USER_PATH,
            "retail_products_path":RETAIL_PRODUCTS_PATH, 
            "transactions_path":TRANSACTIONS_PATH
        }
    )

    ingest_users_task=PythonOperator(
        task_id='ingest_users',
        op_kwargs={"table": "users", "s3_path":USER_PATH},
        python_callable=ingest_redshift_data
    )

    ingest_retail_products_task=PythonOperator(
        task_id='ingest_retail_products',
        op_kwargs={"table": "retail_products", "s3_path":RETAIL_PRODUCTS_PATH},
        python_callable=ingest_redshift_data
    )

    ingest_transactions_task=PythonOperator(
        task_id='ingest_transactions',
        op_kwargs={"table": "transactions", "s3_path":TRANSACTIONS_PATH},
        python_callable=ingest_redshift_data
    )

    push_purchases_task=PythonOperator(
        task_id='ingest_transactions',
        op_kwargs={"table": "transactions", "s3_path":TRANSACTIONS_PATH},
        python_callable=push_purchases_to_graph
    )

    push_roasts_task=PythonOperator(
        task_id='ingest_transactions',
        op_kwargs={"table": "transactions", "s3_path":TRANSACTIONS_PATH},
        python_callable=push_roasting_profile_to_graph
    )

    generate_data_task >> [ingest_users_task, ingest_retail_products_task, ingest_transactions_task] >> [push_purchases_task, push_purchases_task]

