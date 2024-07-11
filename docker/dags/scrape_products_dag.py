from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from coffee_products import scrape_sm_products, normalize_products_for_ecom, update_with_roasted_product


default_args = {
    "owner": "airflow"
}

with DAG(
    dag_id="scrape_and_make_products",
    default_args=default_args,
    schedule_interval="@monthly"
) as dag:
    
    dark_task=PythonOperator(
        task_id='scrape_dark_roasts',
        python_callable=scrape_sm_products,
        op_kwargs={"search_type": "dark"}
    )

    light_task=PythonOperator(
        task_id='scrape_light_roasts',
        python_callable=scrape_sm_products,
        op_kwargs={"search_type": "light"}
    )

    espresso_task=PythonOperator(
        task_id='scrape_espresso_blends',
        python_callable=scrape_sm_products,
        op_kwargs={"search_type": "espresso"}
    )

    normalize_task=PythonOperator(
        task_id='normalize_coffee_products',
        python_callable=normalize_products_for_ecom
    )

    push_new_roasted_products=PythonOperator(
        task_id='update_graph_with_products',
        python_callable=update_with_roasted_product
    )

    [dark_task, light_task, espresso_task] >> normalize_task >> push_new_roasted_products
