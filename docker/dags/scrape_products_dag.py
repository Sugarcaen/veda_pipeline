from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from coffee_products import scrape_sm_products, normalize_products
from openweather_api import get_openweather_api_data
from db_utils import push_products_to_graph


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
        python_callable=normalize_products
    )

    get_weather_task=PythonOperator(
        task_id='get_location_weather',
        python_callable=get_openweather_api_data
    )

    get_aq_task=PythonOperator(
        task_id='get_location_aq',
        python_callable=get_openweather_api_data
        op_kwargs={"api": "aq"}        
    )

    push_new_products_to_graph=PythonOperator(
        task_id='update_graph_with_products',
        python_callable=push_products_to_graph
    )

    [dark_task, light_task, espresso_task] >> normalize_task >> [get_weather_task, get_weather_task] >> push_new_products_to_graph
