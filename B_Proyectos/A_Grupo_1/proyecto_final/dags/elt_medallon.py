import sys
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
import os

# Configuracion de Rutas de Sistema
PROJECT_ROOT = Path("/opt/airflow")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Importaciones de tus modulos tecnicos en la carpeta /elt
from elt.raw import ingest_raw
from elt.bronze import transform_raw_to_bronze
from elt.silver import (
    transform_products_to_silver,
    transform_carts_to_silver,
    transform_users_to_silver,
)
from elt.dim_categories import build_dim_categories
from elt.dim_products import build_dim_products
from elt.dim_cities import build_dim_cities
from elt.dim_users import build_dim_users
from elt.fact_carts import build_fact_carts
from elt.send_email import send_completion_email

# Configuracion Horaria
TIMEZONE = pendulum.timezone("America/Lima")

# Definicion del Data Lake (Rutas Independientes)
DATA_LAKE_ROOT = PROJECT_ROOT / "data_lake"
RAW_ROOT = DATA_LAKE_ROOT / "raw"
BRONZE_ROOT = DATA_LAKE_ROOT / "bronze"
SILVER_ROOT = DATA_LAKE_ROOT / "silver"
GOLD_PATH = DATA_LAKE_ROOT / "gold"

PRODUCTS_URL = "https://fakestoreapi.com/products"
CARTS_URL = "https://fakestoreapi.com/carts"
USERS_URL = "https://fakestoreapi.com/users"

RAW_PRODUCTS_PATH = RAW_ROOT / "products"
RAW_CARTS_PATH = RAW_ROOT / "carts"
RAW_USERS_PATH = RAW_ROOT / "users"

BRONZE_PRODUCTS_PATH = BRONZE_ROOT / "products"
BRONZE_CARTS_PATH = BRONZE_ROOT / "carts"
BRONZE_USERS_PATH = BRONZE_ROOT / "users"

SILVER_PRODUCTS_PATH = SILVER_ROOT / "products"
SILVER_CARTS_PATH = SILVER_ROOT / "carts"
SILVER_USERS_PATH = SILVER_ROOT / "users"

RAW_PRODUCTS_FILE = "products_raw.json"
RAW_CARTS_FILE = "carts_raw.json"
RAW_USERS_FILE = "users_raw.json"

BRONZE_PRODUCTS_FILE = "products_bronze.parquet"
BRONZE_CARTS_FILE = "carts_bronze.parquet"
BRONZE_USERS_FILE = "users_bronze.parquet"

SILVER_PRODUCTS_FILE = "products_silver.parquet"
SILVER_CARTS_FILE = "carts_silver.parquet"
SILVER_USERS_FILE = "users_silver.parquet"

# Diccionarios de Parametros para las Funciones
RAW_PRODUCTS_PARAMS = {
    "api_url": PRODUCTS_URL,
    "raw_path": RAW_PRODUCTS_PATH / RAW_PRODUCTS_FILE, "timeout": 30
}
RAW_CARTS_PARAMS = {
    "api_url": CARTS_URL,
    "raw_path": RAW_CARTS_PATH / RAW_CARTS_FILE, "timeout": 30
}
RAW_USERS_PARAMS = {
    "api_url": USERS_URL,
    "raw_path": RAW_USERS_PATH / RAW_USERS_FILE, "timeout": 30
}

BRONZE_PRODUCTS_PARAMS = {
    "raw_path": RAW_PRODUCTS_PATH / RAW_PRODUCTS_FILE,
    "bronze_path": BRONZE_PRODUCTS_PATH / BRONZE_PRODUCTS_FILE,
}
BRONZE_CARTS_PARAMS = {
    "raw_path": RAW_CARTS_PATH / RAW_CARTS_FILE,
    "bronze_path": BRONZE_CARTS_PATH / BRONZE_CARTS_FILE,
}
BRONZE_USERS_PARAMS = {
    "raw_path": RAW_USERS_PATH / RAW_USERS_FILE,
    "bronze_path": BRONZE_USERS_PATH / BRONZE_USERS_FILE,
}

SILVER_PRODUCTS_PARAMS = {
    "bronze_path": BRONZE_PRODUCTS_PATH / BRONZE_PRODUCTS_FILE,
    "silver_path": SILVER_PRODUCTS_PATH / SILVER_PRODUCTS_FILE,
}
SILVER_CARTS_PARAMS = {
    "bronze_path": BRONZE_CARTS_PATH / BRONZE_CARTS_FILE,
    "silver_path": SILVER_CARTS_PATH / SILVER_CARTS_FILE,
}
SILVER_USERS_PARAMS = {
    "bronze_path": BRONZE_USERS_PATH / BRONZE_USERS_FILE,
    "silver_path": SILVER_USERS_PATH / SILVER_USERS_FILE,
}

# Parametros para Capa Gold (Modelo Snowflake)
DIM_CATEGORIES_PARAMS = {
    "silver_path": SILVER_PRODUCTS_PATH / SILVER_PRODUCTS_FILE,
    "output_path": GOLD_PATH / "dimensions" / "dim_categories.parquet",
}
DIM_PRODUCTS_PARAMS = {
    "silver_path": SILVER_PRODUCTS_PATH / SILVER_PRODUCTS_FILE,
    "dim_categories_path": GOLD_PATH / "dimensions" / "dim_categories.parquet",
    "output_path": GOLD_PATH / "dimensions" / "dim_products.parquet",
}
DIM_CITIES_PARAMS = {
    "silver_users_path": SILVER_USERS_PATH / SILVER_USERS_FILE,
    "output_path": GOLD_PATH / "dimensions" / "dim_cities.parquet",
}
DIM_USERS_PARAMS = {
    "silver_users_path": SILVER_USERS_PATH / SILVER_USERS_FILE,
    "dim_cities_path": GOLD_PATH / "dimensions" / "dim_cities.parquet",
    "output_path": GOLD_PATH / "dimensions" / "dim_users.parquet",
}
FACT_CARTS_PARAMS = {
    "silver_carts_path": SILVER_CARTS_PATH / SILVER_CARTS_FILE,
    "output_path": GOLD_PATH / "facts" / "fact_carts.parquet",
}


@dag(
    dag_id="elt_medallon_fakestore",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 2, 1, tz=TIMEZONE),
    catchup=False,
    tags=["data_engineering", "fakestore", "parquet"],
)
def elt_medallon_dag():

    @task()
    def ingest_products():
        return ingest_raw(**RAW_PRODUCTS_PARAMS)

    @task()
    def ingest_carts():
        return ingest_raw(**RAW_CARTS_PARAMS)

    @task()
    def ingest_users():
        return ingest_raw(**RAW_USERS_PARAMS)

    @task()
    def bronze_products():
        return transform_raw_to_bronze(**BRONZE_PRODUCTS_PARAMS)

    @task()
    def bronze_carts():
        return transform_raw_to_bronze(**BRONZE_CARTS_PARAMS)

    @task()
    def bronze_users():
        return transform_raw_to_bronze(**BRONZE_USERS_PARAMS)

    @task()
    def silver_products():
        return transform_products_to_silver(**SILVER_PRODUCTS_PARAMS)

    @task()
    def silver_carts():
        return transform_carts_to_silver(**SILVER_CARTS_PARAMS)

    @task()
    def silver_users():
        return transform_users_to_silver(**SILVER_USERS_PARAMS)

    @task()
    def dim_categories():
        return build_dim_categories(**DIM_CATEGORIES_PARAMS)

    @task()
    def dim_products():
        return build_dim_products(**DIM_PRODUCTS_PARAMS)

    @task()
    def dim_cities():
        return build_dim_cities(**DIM_CITIES_PARAMS)

    @task()
    def dim_users():
        return build_dim_users(**DIM_USERS_PARAMS)

    @task()
    def fact_carts():
        return build_fact_carts(**FACT_CARTS_PARAMS)

    @task.sensor(task_id="wait_for_fact_file", poke_interval=20, timeout=600, mode="poke")
    def check_fact_file_exists():
        path = FACT_CARTS_PARAMS["output_path"]
        if os.path.exists(path):
            print(f"Archivo detectado con exito en: {path}")
            return True
        print(f"Buscando archivo en: {path} ...")
        return False

    @task()
    def notify():
        return send_completion_email()

    # Orquestacion y Flujo de Dependencias
    raw_products = ingest_products()
    raw_carts = ingest_carts()
    raw_users = ingest_users()

    bronze_products_task = bronze_products()
    bronze_carts_task = bronze_carts()
    bronze_users_task = bronze_users()

    silver_products_task = silver_products()
    silver_carts_task = silver_carts()
    silver_users_task = silver_users()

    dim_cat = dim_categories()
    dim_prod = dim_products()
    dim_city = dim_cities()
    dim_user = dim_users()

    fact = fact_carts()

    raw_products >> bronze_products_task >> silver_products_task
    raw_carts >> bronze_carts_task >> silver_carts_task
    raw_users >> bronze_users_task >> silver_users_task

    silver_products_task >> dim_cat
    [silver_products_task, dim_cat] >> dim_prod

    silver_users_task >> dim_city
    [silver_users_task, dim_city] >> dim_user

    [silver_carts_task, dim_prod, dim_user] >> fact
    fact >> check_fact_file_exists() >> notify()


elt_medallon = elt_medallon_dag()
