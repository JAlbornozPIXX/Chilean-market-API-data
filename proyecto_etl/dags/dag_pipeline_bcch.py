"""dag etl del banco central: cada etapa de pipeline_bcch.py es una tarea.

schedule: dias habiles a las 10:00 (america/santiago). el bcch publica dolar y uf al cierre de
cada dia habil; cada run procesa una ventana de 7 dias para recoger correcciones sin duplicar.
recarga de un rango pasado: trigger con {"fecha_desde": "2024-01-01", "fecha_hasta": "2026-11-02"}.

configuracion en airflow (ninguna credencial en el codigo):
  - connection dw_postgres: base postgresql destino
  - variable bcch_token: token de la api del bcch
  - variable bcch_base_url (opcional): url de la api; una incorrecta provoca la falla controlada
al terminar la carga publica ASSET_BCCH, que dispara el dag de indicadores.
"""

from datetime import date, datetime, timedelta, timezone

import pendulum
from airflow.sdk import Param, Variable, dag, get_current_context, task

import comun_airflow as af

CARPETA = "tmp_bcch"


def _rango(context: dict) -> tuple[date, date]:
    """rango a procesar en hora de chile: los params o la fecha del run con 7 dias hacia atras."""
    hoy = pendulum.now("America/Santiago").date()
    params = context["params"]
    if params.get("fecha_desde"):
        # recarga de un rango pasado; solo fecha_desde = desde ese dia hasta hoy
        desde = date.fromisoformat(params["fecha_desde"])
        hasta = date.fromisoformat(params["fecha_hasta"]) if params.get("fecha_hasta") else hoy
        return desde, min(hasta, hoy)
    momento = context.get("logical_date") or context.get("data_interval_end")
    hasta = min(momento.in_timezone("America/Santiago").date(), hoy) if momento else hoy
    return hasta - timedelta(days=7), hasta


@dag(
    dag_id="pipeline_bcch_etl",
    description="ETL de dolar, euro, UF y UTM desde la API del Banco Central",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    schedule="0 10 * * 1-5",
    catchup=False,
    max_active_runs=1,  # los archivos intermedios son compartidos entre runs
    default_args={
        "owner": "grupo_9",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": af.on_failure_callback,
        "on_retry_callback": af.on_retry_callback,
    },
    params={
        "fecha_desde": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Recarga de un rango pasado (junto con fecha_hasta)",
        ),
        "fecha_hasta": Param(None, type=["null", "string"], format="date"),
    },
    tags=["bcch", "etl", "inf522"],
)
def pipeline_bcch_etl() -> None:
    """etl del bcch: crear_modelo -> validar_api -> extract -> transform -> calidad -> load."""

    @task
    def crear_modelo() -> None:
        import pipeline_bcch as p

        p.crear_modelo(af.engine())

    @task(retries=2, execution_timeout=timedelta(minutes=2))
    def validar_api() -> None:
        import pipeline_bcch as p

        p.validar_api(af.credencial("bcch_token"), Variable.get("bcch_base_url", default=p.BCCH_URL))

    @task
    def extract() -> None:
        import pipeline_bcch as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        desde, hasta = _rango(context)
        url = Variable.get("bcch_base_url", default=p.BCCH_URL)
        raw = p.extract_series(af.credencial("bcch_token"), desde, hasta, url)
        filas = p.load_raw(af.engine(), raw, context["run_id"])
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="raw.bcch_series",
            filas_leidas=p.contar_observaciones(raw),
            filas_cargadas=filas,
        )

    @task
    def transform() -> None:
        import pipeline_bcch as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        observaciones = p.transform_observaciones(p.read_raw(af.engine(), context["run_id"]))
        af.guardar(observaciones, CARPETA, "bcch_observacion")
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="staging.bcch_observacion",
            filas_leidas=len(observaciones),
            filas_rechazadas=observaciones["es_nulo"].sum(),
            pct_nulos=p.pct_nulos(observaciones),
        )

    @task
    def calidad() -> None:
        import pipeline_bcch as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        observaciones = af.leer(CARPETA, "bcch_observacion")
        p.check_calidad(observaciones)
        af.auditar(
            context, "SUCCESS", inicio=inicio, filas_leidas=len(observaciones), pct_nulos=p.pct_nulos(observaciones)
        )

    @task(outlets=[af.ASSET_BCCH])
    def load() -> None:
        import pipeline_bcch as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        desde, hasta = _rango(context)
        engine = af.engine()
        observaciones = af.leer(CARPETA, "bcch_observacion")

        filas = p.load_staging(engine, observaciones)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="staging.bcch_observacion",
            filas_leidas=len(observaciones),
            filas_cargadas=filas,
            pct_nulos=p.pct_nulos(observaciones),
        )

        inicio = datetime.now(timezone.utc)
        leidas, cargadas = p.load_dw(engine, p.build_dim_tiempo(desde, hasta), p.build_dim_serie(), desde, hasta)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="dw.fact_indicador_economico",
            filas_leidas=leidas,
            filas_cargadas=cargadas,
            filas_rechazadas=leidas - cargadas,
        )

    crear_modelo() >> validar_api() >> extract() >> transform() >> calidad() >> load()


pipeline_bcch_etl()
