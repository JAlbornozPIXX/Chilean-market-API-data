"""utilidades de airflow que comparten los dags etl.

conexion a la base (connection dw_postgres), credenciales enmascaradas en los logs,
archivos intermedios entre tareas, auditoria en audit.etl_audit y callbacks de falla y reintento.

compatibilidad de versiones: el proyecto corre en airflow 3.x (docker-compose.yml usa 3.3.2) y tambien
en airflow 2.x a partir de 2.10 (por los assets con condicion OR, que son datasets en 2.x).
todo lo que cambia entre versiones esta en este modulo: los dags importan dag, task, Param, etc.
desde aqui y nunca desde airflow directamente.
"""

import logging
from pathlib import Path

from sqlalchemy.engine import Engine

try:  # airflow 3.x: task sdk
    from airflow.sdk import Asset, Connection, Param, Variable, dag, get_current_context, task
    from airflow.sdk.log import mask_secret

    AIRFLOW_3 = True
except ImportError:  # airflow 2.x (>= 2.10)
    from airflow.datasets import Dataset as Asset
    from airflow.decorators import dag, task
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
    from airflow.utils.log.secrets_masker import mask_secret

    AIRFLOW_3 = False

__all__ = [
    "AIRFLOW_3", "ASSET_BCCH", "ASSET_MP", "Param", "auditar", "conexion", "credencial", "dag", "engine",
    "get_current_context", "guardar", "leer", "momento_programado", "on_failure_callback", "on_retry_callback",
    "task", "variable",
]  # fmt: skip

log = logging.getLogger(__name__)

SALIDA = Path(__file__).parent / "salida"

# assets: cada dag etl publica el suyo al terminar la carga y el dag de indicadores
# corre cuando se actualiza cualquiera de los dos (schedule=ASSET_BCCH | ASSET_MP)
ASSET_BCCH = Asset("postgres://dw_postgres:5432/etl_bcc/dw/fact_indicador_economico")
ASSET_MP = Asset("postgres://dw_postgres:5432/etl_bcc/dw/fact_licitacion")


def guardar(obj: object, carpeta: str, nombre: str) -> None:
    """guarda un objeto (dataframe) para que lo lea la siguiente tarea."""
    import pandas as pd

    tmp = SALIDA / carpeta
    tmp.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(obj, tmp / f"{nombre}.pkl")


def leer(carpeta: str, nombre: str) -> object:
    """lee el objeto que guardo la tarea anterior."""
    import pandas as pd

    return pd.read_pickle(SALIDA / carpeta / f"{nombre}.pkl")


def conexion(conn_id: str) -> object:
    """connection de airflow (host, port, schema, login y password en ambas versiones)."""
    return Connection.get(conn_id) if AIRFLOW_3 else BaseHook.get_connection(conn_id)


def variable(clave: str, default: object = None) -> object:
    """variable de airflow; si no existe devuelve default (el argumento se llama distinto en 2.x y 3.x)."""
    if AIRFLOW_3:
        return Variable.get(clave, default=default)
    return Variable.get(clave, default_var=default)


def momento_programado(context: dict, alternativa: str = "data_interval_start") -> object:
    """fecha y hora en que el cron programo el run; none si el run es manual.

    airflow 3: el cron no tiene intervalo, asi que logical_date es la hora programada.
    airflow 2: logical_date es el inicio del intervalo; la hora programada es el fin del intervalo
    (data_interval_end) y un run manual no tiene hora programada.
    """
    if AIRFLOW_3:
        return context.get("logical_date") or context.get(alternativa)
    if context["dag_run"].run_type == "manual":
        return None
    return context.get("data_interval_end")


def engine() -> Engine:
    """engine de sqlalchemy con los datos de la connection dw_postgres."""
    import pipeline_bcch as p

    c = conexion("dw_postgres")
    return p.get_engine(host=c.host, port=c.port, database=c.schema, user=c.login, password=c.password)


def credencial(clave: str) -> str:
    """lee una variable con una credencial y la enmascara (***) en los logs de la tarea."""
    valor = Variable.get(clave)
    mask_secret(valor)
    return valor


def auditar(context: dict, estado: str, **campos: object) -> None:
    """registra una fila en audit.etl_audit para la tarea actual."""
    import pipeline_bcch as p

    ti = context["ti"]
    p.registrar_auditoria(
        engine(),
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context["run_id"],
        estado=estado,
        intento=ti.try_number,
        **campos,
    )


def _registrar_falla(context: dict, estado: str) -> None:
    """guarda la falla o el reintento en la auditoria y deja una alerta en el log."""
    exc = context.get("exception")
    try:
        inicio = getattr(context["ti"], "start_date", None)
        auditar(context, estado, inicio=inicio, mensaje=str(exc)[:2000] if exc else None)
    except Exception:  # la auditoria nunca debe romper el callback
        log.exception("No se pudo registrar el estado %s en audit.etl_audit", estado)
    log.error(
        "[ALERTA] %s.%s run=%s estado=%s error=%s",
        context["ti"].dag_id, context["ti"].task_id, context["run_id"], estado, exc,
    )  # fmt: skip


def on_failure_callback(context: dict) -> None:
    """callback de airflow cuando una tarea falla definitivamente."""
    _registrar_falla(context, "FAILED")


def on_retry_callback(context: dict) -> None:
    """callback de airflow cuando una tarea queda en reintento."""
    _registrar_falla(context, "RETRY")
