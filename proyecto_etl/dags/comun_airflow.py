"""utilidades de airflow que comparten los dags etl.

conexion a la base (connection dw_postgres), credenciales enmascaradas en los logs,
archivos intermedios entre tareas, auditoria en audit.etl_audit y callbacks de falla y reintento.
"""

import logging
from pathlib import Path

from airflow.sdk import Asset, Connection, Variable
from airflow.sdk.log import mask_secret
from sqlalchemy.engine import Engine

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


def engine() -> Engine:
    """engine de sqlalchemy con los datos de la connection dw_postgres."""
    import pipeline_bcch as p

    c = Connection.get("dw_postgres")
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
