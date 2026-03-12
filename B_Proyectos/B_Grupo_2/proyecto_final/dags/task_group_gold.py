from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Importamos funciones de GOLD
from elt.gold.dim_date import build_dim_date
from elt.gold.dim_location import build_dim_location
from elt.gold.dim_event_type import build_dim_event_type
from elt.gold.dim_status import build_dim_status
from elt.gold.fact_earthquakes import build_fact_earthquakes


def build_gold_group(group_id: str = "gold_layer") -> TaskGroup:
    """
    Crea el TaskGroup de la capa GOLD:
    - Construye dimensiones
    - Luego construye la tabla fact
    """

    with TaskGroup(group_id=group_id) as gold_group:

        # ===============================
        # DIMENSIONES
        # ===============================
        dim_date_task = PythonOperator(
            task_id="build_dim_date",
            python_callable=build_dim_date,
        )

        dim_location_task = PythonOperator(
            task_id="build_dim_location",
            python_callable=build_dim_location,
        )

        dim_event_type_task = PythonOperator(
            task_id="build_dim_event_type",
            python_callable=build_dim_event_type,
        )

        dim_status_task = PythonOperator(
            task_id="build_dim_status",
            python_callable=build_dim_status,
        )

        # ===============================
        # FACT
        # ===============================
        fact_task = PythonOperator(
            task_id="build_fact_earthquakes",
            python_callable=build_fact_earthquakes,
        )

        # ===============================
        # DEPENDENCIAS INTERNAS
        # ===============================
        [
            dim_date_task,
            dim_location_task,
            dim_event_type_task,
            dim_status_task,
        ] >> fact_task

    return gold_group