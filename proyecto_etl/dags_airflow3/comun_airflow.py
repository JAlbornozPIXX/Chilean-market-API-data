"""Utilidades de Airflow que comparten los DAGs ETL: conexión a la base (Connection
dw_postgres), credenciales enmascaradas en los logs, archivos intermedios entre tareas,
auditoría en audit.etl_audit y callbacks de falla y reintento."""
import logging
from pathlib import Path

from airflow.sdk import Asset, Connection, Variable
from airflow.sdk.log import mask_secret

log = logging.getLogger(__name__)

SALIDA = Path(__file__).parent / "salida"

# Assets: cada DAG ETL publica el suyo al terminar la carga y el DAG de indicadores corre
# cuando se actualiza cualquiera de los dos (schedule=ASSET_BCCH | ASSET_MP)
ASSET_BCCH = Asset("postgres://dw_postgres:5432/etl_bcc/dw/fact_indicador_economico")
ASSET_MP = Asset("postgres://dw_postgres:5432/etl_bcc/dw/fact_licitacion")


def guardar(obj, carpeta, nombre):
    import pandas as pd
    tmp = SALIDA / carpeta
    tmp.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(obj, tmp / f"{nombre}.pkl")


def leer(carpeta, nombre):
    import pandas as pd
    return pd.read_pickle(SALIDA / carpeta / f"{nombre}.pkl")


def engine():
    import pipeline_bcch as p
    c = Connection.get("dw_postgres")
    return p.get_engine(host=c.host, port=c.port, database=c.schema, user=c.login, password=c.password)


def credencial(clave):
    """Lee una Variable con una credencial y la enmascara (***) en los logs de la tarea."""
    valor = Variable.get(clave)
    mask_secret(valor)
    return valor


def auditar(context, estado, **campos):
    import pipeline_bcch as p
    ti = context["ti"]
    p.registrar_auditoria(engine(), dag_id=ti.dag_id, task_id=ti.task_id, run_id=context["run_id"],
                          estado=estado, intento=ti.try_number, **campos)


def _registrar_falla(context, estado):
    exc = context.get("exception")
    try:
        auditar(context, estado, inicio=getattr(context["ti"], "start_date", None),
                mensaje=str(exc)[:2000] if exc else None)
    except Exception:  # la auditoría nunca debe romper el callback
        log.exception("No se pudo registrar el estado %s en audit.etl_audit", estado)
    log.error("[ALERTA] %s.%s run=%s estado=%s error=%s", context["ti"].dag_id, context["ti"].task_id,
              context["run_id"], estado, exc)


def on_failure_callback(context):
    _registrar_falla(context, "FAILED")


def on_retry_callback(context):
    _registrar_falla(context, "RETRY")
