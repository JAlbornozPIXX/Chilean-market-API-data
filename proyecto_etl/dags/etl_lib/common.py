"""Utilidades compartidas: conexión a PostgreSQL, cliente HTTP robusto,
auditoría (audit.etl_audit) y callback de fallas.

Ninguna credencial vive en el código: la conexión a la BD es la Airflow
Connection `dw_postgres` y las claves de API son Airflow Variables, ambas
inyectadas por variables de entorno desde el archivo .env (no se entrega).
"""
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

PG_CONN_ID = "dw_postgres"
HTTP_TIMEOUT = (10, 60)  # (conexión, lectura) en segundos


# ----------------------------------------------------------------- BD
def get_pg_conn():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


@contextmanager
def pg_cursor():
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def asegurar_dim_tiempo(cur, desde, hasta):
    """Puebla dw.dim_tiempo para el rango (idempotente)."""
    cur.execute(
        """
        INSERT INTO dw.dim_tiempo
        SELECT to_char(d,'YYYYMMDD')::int, d::date,
               extract(year from d), extract(quarter from d), extract(month from d),
               (ARRAY['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto',
                      'Septiembre','Octubre','Noviembre','Diciembre'])[extract(month from d)],
               extract(day from d), extract(isodow from d), extract(isodow from d) IN (6,7),
               to_char(d,'YYYY-MM')
        FROM generate_series(%s::date, %s::date, interval '1 day') d
        ON CONFLICT (tiempo_key) DO NOTHING
        """,
        (desde, hasta),
    )


# ----------------------------------------------------------------- HTTP
class ApiError(Exception):
    """Error de negocio devuelto por la API (HTTP 200 con código de error)."""


def http_session(total_retries=4, backoff=2.0):
    """Sesión con reintentos exponenciales para 429 (límite de uso) y 5xx.
    Respeta la cabecera Retry-After cuando la API la envía."""
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "INF522-ETL/1.0"})
    return s


def get_json(session, url, params, min_interval=0.0):
    """GET con timeout, verificación de estado HTTP y control de ritmo
    (min_interval segundos entre llamadas para no exceder cuotas)."""
    if min_interval:
        time.sleep(min_interval)
    resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
    if resp.status_code >= 400:
        raise requests.HTTPError(
            f"HTTP {resp.status_code} en {url}: {resp.text[:300]}", response=resp
        )
    try:
        return resp.status_code, resp.json()
    except ValueError as exc:
        raise ApiError(f"Respuesta no JSON desde {url}: {resp.text[:200]}") from exc


# ----------------------------------------------------------------- Auditoría
class Auditoria:
    """Registra una fila en audit.etl_audit por cada tarea ejecutada."""

    def __init__(self, context, tabla_destino=None):
        ti = context["ti"]
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.run_id = context["run_id"]
        self.intento = ti.try_number
        self.tabla = tabla_destino
        self.inicio = datetime.now(timezone.utc)
        self.filas_leidas = 0
        self.filas_cargadas = 0
        self.filas_rechazadas = 0
        self.pct_nulos = None

    def cerrar(self, estado="SUCCESS", mensaje=None):
        fin = datetime.now(timezone.utc)
        with pg_cursor() as cur:
            cur.execute(
                """INSERT INTO audit.etl_audit
                   (dag_id, task_id, run_id, tabla_destino, filas_leidas, filas_cargadas,
                    filas_rechazadas, pct_nulos, inicio, fin, duracion_seg, estado, intento, mensaje)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (self.dag_id, self.task_id, self.run_id, self.tabla, self.filas_leidas,
                 self.filas_cargadas, self.filas_rechazadas, self.pct_nulos, self.inicio,
                 fin, (fin - self.inicio).total_seconds(), estado, self.intento, mensaje),
            )
        log.info("AUDIT %s.%s -> %s leidas=%s cargadas=%s nulos=%s%%",
                 self.dag_id, self.task_id, estado, self.filas_leidas,
                 self.filas_cargadas, self.pct_nulos)


def _registrar_estado(context, estado):
    ti = context["task_instance"]
    exc = context.get("exception")
    try:
        with pg_cursor() as cur:
            cur.execute(
                """INSERT INTO audit.etl_audit
                   (dag_id, task_id, run_id, inicio, fin, duracion_seg, estado, intento, mensaje)
                   VALUES (%s,%s,%s,%s,now(),%s,%s,%s,%s)""",
                (ti.dag_id, ti.task_id, context["run_id"], ti.start_date or datetime.now(timezone.utc),
                 ti.duration, estado, ti.try_number, str(exc)[:2000] if exc else None),
            )
    except Exception:  # la auditoría nunca debe romper el callback
        log.exception("No se pudo registrar el estado %s en etl_audit", estado)
    log.error("[ALERTA] %s.%s run=%s intento=%s estado=%s error=%s",
              ti.dag_id, ti.task_id, context["run_id"], ti.try_number, estado, exc)


def on_failure_callback(context):
    _registrar_estado(context, "FAILED")


def on_retry_callback(context):
    _registrar_estado(context, "RETRY")


def pct_nulos(filas, campos):
    """% de celdas nulas sobre los campos indicados."""
    if not filas:
        return 0.0
    total = len(filas) * len(campos)
    nulos = sum(1 for f in filas for c in campos if f.get(c) is None)
    return round(100.0 * nulos / total, 3)
