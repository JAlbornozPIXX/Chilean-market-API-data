"""dag etl de mercado publico (licitaciones): cada etapa de pipeline_mercadopublico.py es una tarea.

schedule: todos los dias a las 06:00 (america/santiago), procesando el dia anterior completo;
las licitaciones se mueven todos los dias, incluso los fines de semana.
catchup=True: al activarlo crea un run por cada dia pendiente desde start_date (es seguro
porque todo es idempotente). rango puntual: trigger con {"fecha_desde": "2026-10-05", "fecha_hasta": "2026-10-06"}.

configuracion en airflow (ninguna credencial en el codigo):
  - connection dw_postgres: base postgresql destino
  - variable mp_ticket: ticket de la api de mercado publico (se enmascara en los logs)
  - variable mp_max_detalle (opcional, 150 por defecto): detalles por run;
    cuota diaria del ticket: runs * (2 + mp_max_detalle) < 10.000
  - variable mp_base_url (opcional): url de la api; una incorrecta provoca la falla controlada
al terminar la carga publica ASSET_MP, que dispara el dag de indicadores.
"""

from datetime import date, datetime, timedelta, timezone

import pendulum

import comun_airflow as af
from comun_airflow import Param, dag, get_current_context, task

CARPETA = "tmp_mercadopublico"


def _rango(context: dict) -> tuple[date, date]:
    """rango a procesar en hora de chile: los params o el dia anterior a la fecha del run.

    en airflow 3 el cron no tiene intervalo, por eso el run de las 06:00 procesa el dia anterior a su fecha
    logica; en airflow 2 la fecha programada es el fin del intervalo (ver af.momento_programado).
    nunca pasa de hoy: la api rechaza fechas futuras ("La fecha es mayor a la actual").
    """
    hoy = pendulum.now("America/Santiago").date()
    params = context["params"]
    if params.get("fecha_desde"):
        # recarga de un rango pasado; solo fecha_desde = ese dia
        desde = date.fromisoformat(params["fecha_desde"])
        hasta = date.fromisoformat(params["fecha_hasta"]) if params.get("fecha_hasta") else desde
        return desde, min(hasta, hoy)
    momento = af.momento_programado(context, alternativa="data_interval_start")
    dia = (momento.in_timezone("America/Santiago").date() if momento else hoy) - timedelta(days=1)
    return dia, dia


def _url() -> str:
    """url de la api: la variable mp_base_url o la oficial."""
    import pipeline_mercadopublico as p

    return af.variable("mp_base_url", p.MP_URL)


@dag(
    dag_id="pipeline_mercadopublico_etl",
    description="ETL de licitaciones diarias y su detalle desde la API de Mercado Publico",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Santiago"),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,  # el ticket no admite peticiones simultaneas (codigo 10500)
    default_args={
        "owner": "grupo_9",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=45),  # la descarga del detalle tarda varios minutos
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
    tags=["mercadopublico", "etl", "inf522"],
)
def pipeline_mercadopublico_etl() -> None:
    """etl de mercado publico: crear_modelo -> validar_api -> extract -> transform -> calidad -> load."""

    @task
    def crear_modelo() -> None:
        import pipeline_mercadopublico as p

        p.crear_modelo(af.engine())

    @task(retries=2, execution_timeout=timedelta(minutes=2))
    def validar_api() -> None:
        import pipeline_mercadopublico as p

        p.validar_api(af.credencial("mp_ticket"), _url())

    @task
    def extract() -> None:
        import pipeline_mercadopublico as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        desde, hasta = _rango(context)
        ticket, url, engine = af.credencial("mp_ticket"), _url(), af.engine()

        listados = p.extract_listados(ticket, desde, hasta, url)
        p.load_raw_listados(engine, listados, context["run_id"])
        max_detalle = int(af.variable("mp_max_detalle", p.MAX_DETALLE))
        pendientes = p.codigos_pendientes(engine, listados, desde, hasta, max_detalle)
        descargados, omitidos = p.extract_detalles(engine, ticket, pendientes, context["run_id"], url)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="raw.mp_licitaciones_dia / raw.mp_licitacion_detalle",
            filas_leidas=p.contar_licitaciones(listados),
            filas_cargadas=len(listados) + descargados,
            filas_rechazadas=omitidos,
            mensaje=f"listados={len(listados)} detalles descargados={descargados} de {len(pendientes)} pedidos",
        )

    @task
    def transform() -> None:
        import pipeline_mercadopublico as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        licitaciones, rechazadas = p.transform_licitaciones(*p.read_raw(af.engine(), context["run_id"]))
        af.guardar(licitaciones, CARPETA, "mp_licitacion")
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="staging.mp_licitacion",
            filas_leidas=len(licitaciones) + rechazadas,
            filas_rechazadas=rechazadas,
            pct_nulos=p.pct_nulos_detalle(licitaciones),
        )

    @task
    def calidad() -> None:
        import pipeline_mercadopublico as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        licitaciones = af.leer(CARPETA, "mp_licitacion")
        p.check_calidad(licitaciones)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            filas_leidas=len(licitaciones),
            pct_nulos=p.pct_nulos_detalle(licitaciones),
        )

    @task(outlets=[af.ASSET_MP])
    def load() -> None:
        import pipeline_mercadopublico as p

        context, inicio = get_current_context(), datetime.now(timezone.utc)
        engine = af.engine()
        licitaciones = af.leer(CARPETA, "mp_licitacion")

        filas = p.load_staging(engine, licitaciones)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="staging.mp_licitacion",
            filas_leidas=len(licitaciones),
            filas_cargadas=filas,
            pct_nulos=p.pct_nulos_detalle(licitaciones),
        )

        inicio = datetime.now(timezone.utc)
        leidas, cargadas, pct_sin_conversion = p.load_dw(engine)
        af.auditar(
            context,
            "SUCCESS",
            inicio=inicio,
            tabla_destino="dw.fact_licitacion",
            filas_leidas=leidas,
            filas_cargadas=cargadas,
            pct_nulos=pct_sin_conversion,
            mensaje="pct_nulos = % de montos que no se pudieron convertir a CLP",
        )

    crear_modelo() >> validar_api() >> extract() >> transform() >> calidad() >> load()


pipeline_mercadopublico_etl()
