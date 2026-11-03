"""DAG de indicadores de gestión: cada etapa de pipeline_kpis.py es una tarea.

Se dispara automáticamente (scheduling por Assets) cuando CUALQUIERA de los DAGs ETL termina
su carga: ASSET_BCCH | ASSET_MP. Así, si una fuente falla, los KPIs se recalculan igual con los
datos de la otra fuente y con la última información válida de la caída. No depende de que el
otro DAG termine bien: los tres DAGs son independientes.
"""
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.sdk import dag, get_current_context, task

import comun_airflow as af


@dag(
    dag_id="pipeline_kpis_gestion",
    description="Calcula los 6 indicadores de gestión de compras públicas en el esquema mart",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    schedule=(af.ASSET_BCCH | af.ASSET_MP),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "grupo_9",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": af.on_failure_callback,
        "on_retry_callback": af.on_retry_callback,
    },
    tags=["kpi", "mart", "inf522"],
)
def pipeline_kpis_gestion():

    @task
    def crear_modelo():
        import pipeline_kpis as p
        p.crear_modelo(af.engine())

    @task
    def verificar_frescura():
        import pipeline_kpis as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        ultimas = p.verificar_frescura(af.engine())
        af.auditar(context, "SUCCESS", inicio=inicio,
                   mensaje="; ".join(f"{fuente}: última carga {ultima}" for fuente, ultima in ultimas.items()))

    @task
    def calcular_kpis():
        import pipeline_kpis as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        n_kpi, n_top, pct_nulos = p.calcular_kpis(af.engine())
        af.auditar(context, "SUCCESS", inicio=inicio, tabla_destino="mart.kpi_mensual / mart.top_organismos_mes",
                   filas_leidas=n_kpi, filas_cargadas=n_kpi + n_top, pct_nulos=pct_nulos,
                   mensaje="pct_nulos = % de KPIs sin valor en el mes")

    crear_modelo() >> verificar_frescura() >> calcular_kpis()


pipeline_kpis_gestion()
