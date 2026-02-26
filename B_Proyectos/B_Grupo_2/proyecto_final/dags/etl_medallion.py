from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ===============================
# IMPORTS DE CAPAS
# ===============================

from elt.raw.ingest_raw import run_raw
from elt.bronze.bronze import run_bronze
from elt.silver.silver import run_silver_latest

# TaskGroup GOLD
from task_group_gold import build_gold_group


# ===============================
# CONFIGURACIÓN DAG
# ===============================

default_args = {
    "owner": "juan",
    "retries": 2,
}


with DAG(
    dag_id="etl_medallion_earthquakes",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # Ejecuta diario
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "medallion", "earthquakes"],
) as dag:

    # ===============================
    # RAW
    # ===============================
    raw_task = PythonOperator(
        task_id="raw_layer",
        python_callable=run_raw,
    )

    # ===============================
    # BRONZE
    # ===============================
    bronze_task = PythonOperator(
        task_id="bronze_layer",
        python_callable=run_bronze,
    )

    # ===============================
    # SILVER
    # ===============================
    silver_task = PythonOperator(
        task_id="silver_layer",
        python_callable=run_silver_latest,
    )

    # ===============================
    # GOLD (TaskGroup)
    # ===============================
    gold_group = build_gold_group()

    # ===============================
    # DEPENDENCIAS
    # ===============================
    raw_task >> bronze_task >> silver_task >> gold_group