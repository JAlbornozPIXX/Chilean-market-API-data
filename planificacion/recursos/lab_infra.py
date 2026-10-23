"""DAG de LABORATORIO (Issue 1): verifica la auditoría y los callbacks de etl_lib.common.

Cópialo a proyecto_etl/dags/lab_infra.py. El patrón proyecto_etl/dags/lab_*.py está en
.gitignore: NO se sube al repositorio. Bórralo cuando termines la prueba.

Resultado esperado en audit.etl_audit:
  tarea_ok    SUCCESS  intento 1   (lo escribe Auditoria.cerrar)
  tarea_falla RETRY    intento 1   (lo escribe on_retry_callback)
  tarea_falla FAILED   intento 2   (lo escribe on_failure_callback)
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_lib.common import Auditoria, on_failure_callback, on_retry_callback


def tarea_ok(**context):
    aud = Auditoria(context, "laboratorio")
    aud.filas_leidas = aud.filas_cargadas = 1
    aud.cerrar()


def tarea_falla(**context):
    raise RuntimeError("Falla de laboratorio: así se ve un error registrado por el callback")


with DAG(
    dag_id="lab_infra",
    description="Laboratorio: auditoría, reintentos y callbacks",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    schedule=None,          # solo se ejecuta a mano (Trigger DAG)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10),
                  "on_failure_callback": on_failure_callback, "on_retry_callback": on_retry_callback},
    tags=["laboratorio"],
) as dag:
    PythonOperator(task_id="tarea_ok", python_callable=tarea_ok) >> \
        PythonOperator(task_id="tarea_falla", python_callable=tarea_falla)
