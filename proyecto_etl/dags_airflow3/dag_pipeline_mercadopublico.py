"""DAG ETL de Mercado Público (licitaciones): cada etapa de pipeline_mercadopublico.py es una tarea.

Schedule: todos los días a las 06:00 (America/Santiago), procesando el día anterior completo.
Las licitaciones se mueven todos los días, incluso los fines de semana.
catchup=True: al activarlo crea un run por cada día pendiente desde start_date (recarga
histórica automática y segura, porque todo es idempotente). Para un rango puntual:
Trigger DAG con {"fecha_desde": "2026-10-05", "fecha_hasta": "2026-10-06"}.

Configuración en Airflow (ninguna credencial en el código):
  - Connection `dw_postgres`: base PostgreSQL destino
  - Variable `mp_ticket`: ticket de la API de Mercado Público (se enmascara en los logs)
  - Variable `mp_max_detalle` (opcional, por defecto 150): detalles por run. Cuota diaria del
    ticket: runs x (2 + mp_max_detalle) < 10.000. Para pruebas conviene 20.
  - Variable `mp_base_url` (opcional): URL de la API; una incorrecta provoca la falla controlada.
Al terminar la carga publica af.ASSET_MP, que dispara el DAG de indicadores.
"""
from datetime import date, datetime, timedelta, timezone

import pendulum
from airflow.sdk import Param, Variable, dag, get_current_context, task

import comun_airflow as af

CARPETA = "tmp_mercadopublico"


def _rango(context):
    """Rango a procesar, en hora de Chile: params (recarga de un rango pasado; solo fecha_desde
    = ese día) o el día anterior a la fecha del run. En Airflow 3 el cron no tiene intervalo
    (inicio = fin = hora del run), por eso el run de las 06:00 procesa el día anterior completo.
    Nunca pasa de hoy: la API rechaza fechas futuras ("La fecha es mayor a la actual")."""
    hoy = pendulum.now("America/Santiago").date()
    params = context["params"]
    if params.get("fecha_desde"):
        desde = date.fromisoformat(params["fecha_desde"])
        hasta = date.fromisoformat(params["fecha_hasta"]) if params.get("fecha_hasta") else desde
        return desde, min(hasta, hoy)
    momento = context.get("logical_date") or context.get("data_interval_start")
    dia = (momento.in_timezone("America/Santiago").date() if momento else hoy) - timedelta(days=1)
    return dia, dia


def _url():
    import pipeline_mercadopublico as p
    return Variable.get("mp_base_url", default=p.MP_URL)


@dag(
    dag_id="pipeline_mercadopublico_etl",
    description="ETL de licitaciones diarias y su detalle desde la API de Mercado Público",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Santiago"),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,          # el ticket no admite peticiones simultáneas (Codigo 10500)
    default_args={
        "owner": "grupo_9",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=45),     # la descarga del detalle tarda varios minutos
        "on_failure_callback": af.on_failure_callback,
        "on_retry_callback": af.on_retry_callback,
    },
    params={
        "fecha_desde": Param(None, type=["null", "string"], format="date",
                             description="Recarga de un rango pasado (junto con fecha_hasta)"),
        "fecha_hasta": Param(None, type=["null", "string"], format="date"),
    },
    tags=["mercadopublico", "etl", "inf522"],
)
def pipeline_mercadopublico_etl():

    @task
    def crear_modelo():
        import pipeline_mercadopublico as p
        p.crear_modelo(af.engine())

    @task(retries=2, execution_timeout=timedelta(minutes=2))
    def validar_api():
        import pipeline_mercadopublico as p
        p.validar_api(af.credencial("mp_ticket"), _url())

    @task
    def extract():
        import pipeline_mercadopublico as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        desde, hasta = _rango(context)
        ticket, url, engine = af.credencial("mp_ticket"), _url(), af.engine()

        listados = p.extract_listados(ticket, desde, hasta, url)
        p.load_raw_listados(engine, listados, context["run_id"])
        pendientes = p.codigos_pendientes(engine, listados, desde, hasta,
                                          int(Variable.get("mp_max_detalle", default=p.MAX_DETALLE)))
        descargados, omitidos = p.extract_detalles(engine, ticket, pendientes, context["run_id"], url)
        af.auditar(context, "SUCCESS", inicio=inicio,
                   tabla_destino="raw.mp_licitaciones_dia / raw.mp_licitacion_detalle",
                   filas_leidas=p.contar_licitaciones(listados), filas_cargadas=len(listados) + descargados,
                   filas_rechazadas=omitidos,
                   mensaje=f"listados={len(listados)} detalles descargados={descargados} de {len(pendientes)} pedidos")

    @task
    def transform():
        import pipeline_mercadopublico as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        licitaciones, rechazadas = p.transform_licitaciones(*p.read_raw(af.engine(), context["run_id"]))
        af.guardar(licitaciones, CARPETA, "mp_licitacion")
        af.auditar(context, "SUCCESS", inicio=inicio, tabla_destino="staging.mp_licitacion",
                   filas_leidas=len(licitaciones) + rechazadas, filas_rechazadas=rechazadas,
                   pct_nulos=p.pct_nulos_detalle(licitaciones))

    @task
    def calidad():
        import pipeline_mercadopublico as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        licitaciones = af.leer(CARPETA, "mp_licitacion")
        p.check_calidad(licitaciones)
        af.auditar(context, "SUCCESS", inicio=inicio, filas_leidas=len(licitaciones),
                   pct_nulos=p.pct_nulos_detalle(licitaciones))

    @task(outlets=[af.ASSET_MP])
    def load():
        import pipeline_mercadopublico as p
        context, inicio = get_current_context(), datetime.now(timezone.utc)
        engine = af.engine()
        licitaciones = af.leer(CARPETA, "mp_licitacion")

        filas = p.load_staging(engine, licitaciones)
        af.auditar(context, "SUCCESS", inicio=inicio, tabla_destino="staging.mp_licitacion",
                   filas_leidas=len(licitaciones), filas_cargadas=filas,
                   pct_nulos=p.pct_nulos_detalle(licitaciones))

        inicio = datetime.now(timezone.utc)
        leidas, cargadas, pct_sin_conversion = p.load_dw(engine)
        af.auditar(context, "SUCCESS", inicio=inicio, tabla_destino="dw.fact_licitacion",
                   filas_leidas=leidas, filas_cargadas=cargadas, pct_nulos=pct_sin_conversion,
                   mensaje="pct_nulos = % de montos que no se pudieron convertir a CLP")

    crear_modelo() >> validar_api() >> extract() >> transform() >> calidad() >> load()


pipeline_mercadopublico_etl()
