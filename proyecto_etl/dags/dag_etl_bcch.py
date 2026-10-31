#DAG 1 - ETL Banco Central de Chile (indicadores económicos).
#Schedule: días hábiles 10:00 (America/Santiago). El BCCh publica el dólar
#observado y la UF al cierre de cada día hábil; con 7 días de holgura el run
#recoge correcciones o publicaciones tardías sin duplicar (upsert).
#Recarga histórica: Trigger DAG w/ config {"fecha_desde":"2024-01-01","fecha_hasta":"<hoy>"}
#(desde 2024 porque hay licitaciones con movimiento en 2026 que se publicaron en 2024)
#o airflow dags backfill`.

from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from etl_lib import bcch
from etl_lib.common import on_failure_callback, on_retry_callback

DATASET_BCCH = Dataset("postgres://postgres:5432/dw_gestion/dw/fact_indicador_economico")

argumentos_por_defecto = {
    "owner": "grupo_9",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id="etl_bcch_indicadores",
    description="Extrae dólar, euro, UF y UTM desde la API del Banco Central",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    schedule="0 10 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args=argumentos_por_defecto,
    params={"fecha_desde": Param(None, type=["null", "string"], format="date"),
            "fecha_hasta": Param(None, type=["null", "string"], format="date")},
    tags=["etl", "bcch", "inf522"],
) as dag:
    validar = PythonOperator(task_id="validar_api", python_callable=bcch.validar_api,
                            retries=2, execution_timeout=timedelta(minutes=2))
    extraer = PythonOperator(task_id="extraer_raw", python_callable=bcch.extraer)
    transformar = PythonOperator(task_id="transformar_staging", python_callable=bcch.transformar)
    cargar = PythonOperator(task_id="cargar_dw", python_callable=bcch.cargar_dw)
    calidad = PythonOperator(task_id="control_calidad", python_callable=bcch.control_calidad,
                            outlets=[DATASET_BCCH])

    validar >> extraer >> transformar >> cargar >> calidad