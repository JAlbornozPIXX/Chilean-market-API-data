"""dag 2 - etl mercado publico (licitaciones).

schedule diario a las 06:00 (america/santiago) procesando el dia anterior completo.
las licitaciones se publican todos los dias, incluso fines de semana.
catchup=True permite recargar dias pasados de forma idempotente.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from etl_lib import mercadopublico as mp
from etl_lib.common import on_failure_callback, on_retry_callback

#aviso que publica el dag al terminar bien; dispara el dag de kpis (misma uri que en dag_kpis.py)
DS_MP = Dataset("postgres://postgres:5432/dw_gestion/dw/fact_licitacion")

default_args = {
    "owner": "grupo_inf522",
    "retries": 3,                                   #reintentos por tarea
    "retry_delay": timedelta(minutes=2),            #espera inicial entre reintentos
    "retry_exponential_backoff": True,              #la espera crece en cada reintento
    "max_retry_delay": timedelta(minutes=15),       #tope de la espera
    "execution_timeout": timedelta(minutes=45),     #evita tareas colgadas
    "on_failure_callback": on_failure_callback,     #registra FAILED en audit.etl_audit
    "on_retry_callback": on_retry_callback,         #registra RETRY en audit.etl_audit
}

with DAG(
    dag_id="etl_mercadopublico_licitaciones",
    description="Extrae licitaciones diarias y su detalle desde la API de Mercado Público",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Santiago"),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,          #evita peticiones simultaneas con el mismo ticket
    default_args=default_args,
    #parametros opcionales para recargar un rango pasado (trigger dag w/ config)
    params={"fecha_desde": Param(None, type=["null", "string"], format="date"),
            "fecha_hasta": Param(None, type=["null", "string"], format="date")},
    tags=["etl", "mercadopublico", "inf522"],
) as dag:
    #validacion previa: falla rapido si la api o el ticket no responden
    validar = PythonOperator(task_id="validar_api", python_callable=mp.validar_api,
                             retries=2, execution_timeout=timedelta(minutes=2))
    extraer = PythonOperator(task_id="extraer_raw", python_callable=mp.extraer)
    transformar = PythonOperator(task_id="transformar_staging", python_callable=mp.transformar)
    cargar = PythonOperator(task_id="cargar_dw", python_callable=mp.cargar_dw)
    #al terminar bien publica el dataset y eso dispara el dag de kpis
    calidad = PythonOperator(task_id="control_calidad", python_callable=mp.control_calidad,
                             outlets=[DS_MP])

    validar >> extraer >> transformar >> cargar >> calidad
