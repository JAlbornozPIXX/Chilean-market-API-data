"""DAG 3 - Indicadores de gestión (mart).

Se dispara automáticamente (Data-aware scheduling) cuando CUALQUIERA de los
DAGs ETL actualiza su tabla de hechos: (DS_BCCH | DS_MP). Así, si una fuente
falla, los KPIs se recalculan igual con los datos de la otra fuente y con la
última información válida de la fuente caída.
"""
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from etl_lib.common import Auditoria, on_failure_callback, on_retry_callback, pg_cursor

log = logging.getLogger(__name__)
DS_BCCH = Dataset("postgres://postgres:5432/dw_gestion/dw/fact_indicador_economico")
DS_MP = Dataset("postgres://postgres:5432/dw_gestion/dw/fact_licitacion")
SQL_KPI = Path("/opt/airflow/sql/kpi/kpis.sql")


def verificar_frescura(**context):
    """No bloquea: deja advertencia si una fuente está desactualizada."""
    with pg_cursor() as cur:
        cur.execute("""SELECT 'bcch', max(cargado_en) FROM dw.fact_indicador_economico
                       UNION ALL SELECT 'mercadopublico', max(cargado_en) FROM dw.fact_licitacion""")
        for fuente, ult in cur.fetchall():
            log.info("Última carga %s: %s", fuente, ult)
            if ult is None or ult < pendulum.now("UTC").subtract(days=3):
                log.warning("Fuente %s desactualizada (>3 días); KPIs con último dato disponible", fuente)


def calcular_kpis(**context):
    aud = Auditoria(context, "mart.kpi_mensual / mart.top_organismos_mes")
    with pg_cursor() as cur:
        cur.execute(SQL_KPI.read_text(encoding="utf-8"))
        cur.execute("SELECT count(*) FROM mart.kpi_mensual")
        aud.filas_cargadas = cur.fetchone()[0]
        cur.execute("SELECT round(100.0*avg((valor IS NULL)::int),3) FROM mart.kpi_mensual")
        aud.pct_nulos = cur.fetchone()[0]
    aud.filas_leidas = aud.filas_cargadas
    aud.cerrar()


with DAG(
    dag_id="kpis_gestion_compras_publicas",
    description="Calcula los indicadores de gestión en el esquema mart",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    schedule=(DS_BCCH | DS_MP),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "grupo_inf522", "retries": 2, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=10),
                  "on_failure_callback": on_failure_callback, "on_retry_callback": on_retry_callback},
    tags=["kpi", "mart", "inf522"],
) as dag:
    frescura = PythonOperator(task_id="verificar_frescura", python_callable=verificar_frescura)
    kpis = PythonOperator(task_id="calcular_kpis", python_callable=calcular_kpis)
    frescura >> kpis
