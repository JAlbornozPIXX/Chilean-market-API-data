"""
PIPELINE ETL BANCO CENTRAL DE CHILE -> MODELO DIMENSIONAL EN POSTGRESQL

Extrae los indicadores económicos (dólar observado, euro, UF y UTM) desde la API
SieteRestWS del Banco Central y los carga por capas, con las tablas de sql/ddl/01_modelo.sql:

    raw      respuesta de la API tal como llega, con fecha y hora de extracción
    staging  observaciones limpias, tipadas y sin duplicados
    dw       modelo dimensional: dim_tiempo, dim_serie y fact_indicador_economico
    mart     indicadores de gestión (los calcula el DAG de indicadores)
    audit    etl_audit: una fila por tarea ejecutada o fallida

Etapas:

    1. VALIDAR    comprobar que la API responde y el token es válido antes de extraer
    2. EXTRACT    una llamada por serie; la respuesta se guarda tal cual en raw
    3. TRANSFORM  limpiar las observaciones de raw: fechas, decimales con coma, nulos y duplicados
    4. CALIDAD    validar claves, series completas y rangos antes de cargar al modelo
    5. LOAD       staging y dw con INSERT ... ON CONFLICT: volver a ejecutar no duplica registros

Cada etapa es una función con entradas y salidas explícitas; main() las ejecuta en orden
y dag_pipeline_bcch.py convierte cada una en una tarea de Airflow.

Sin credenciales en el código: en Airflow el token es la Variable `bcch_token` y la base es
la Connection `dw_postgres`; al ejecutar este script directamente se leen de las variables
de entorno BCCH_TOKEN y PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.

Uso:  python pipeline_bcch.py [fecha_desde] [fecha_hasta]      (por defecto 2024-01-01 a hoy)
"""
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import URL
from urllib3.util.retry import Retry

# --- API Banco Central -------------------------------------------------------
# Token gratuito: registro y activación en https://si3.bcentral.cl/Siete/es/Siete/API
BCCH_URL = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"
HTTP_TIMEOUT = (10, 60)            # (conexión, lectura) en segundos
PAUSA_ENTRE_LLAMADAS = 1.0         # segundos entre consultas, para no exceder el límite de uso
FECHA_INICIO = date(2024, 1, 1)    # hay licitaciones con movimiento en 2026 publicadas en 2024


def get_engine(host=None, port=None, database=None, user=None, password=None):
    """Sin argumentos usa las variables de entorno PG*; Airflow le pasa los datos de la
    Connection dw_postgres. No hay clave por defecto: si falta, falla."""
    password = password or os.getenv("PGPASSWORD")
    if not password:
        raise ValueError("Falta la clave de PostgreSQL (Connection dw_postgres o variable PGPASSWORD)")
    url = URL.create("postgresql+psycopg2", username=user or os.getenv("PGUSER", "postgres"),
                     password=password, host=host or os.getenv("PGHOST", "localhost"),
                     port=int(port or os.getenv("PGPORT", "5432")),
                     database=database or os.getenv("PGDATABASE", "etl_bcc"))
    return create_engine(url)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("etl")

# --------------------------------------------------------------------------
# Parámetros del modelo
# --------------------------------------------------------------------------
# serie_id -> (nombre, unidad, moneda con la que se cruza la licitación, frecuencia)
SERIES = {
    "F073.TCO.PRE.Z.D": ("Dólar observado", "CLP/USD", "USD", "D"),
    "F072.CLP.EUR.N.O.D": ("Euro (tipo de cambio)", "CLP/EUR", "EUR", "D"),
    "F073.UFF.PRE.Z.D": ("Unidad de Fomento", "CLP/UF", "CLF", "D"),
    "F073.UTR.PRE.Z.M": ("Unidad Tributaria Mensual", "CLP/UTM", "UTM", "M"),
}
SERIE_VALIDACION = "F073.UFF.PRE.Z.D"   # la UF se publica todos los días: sirve para probar la API
UMBRAL_ATIPICO_PCT = 15.0               # variación diaria sobre la cual un valor se descarta
RANGOS_VALIDOS = {                      # rango razonable para la validación de calidad
    "F073.UFF.PRE.Z.D": (20_000, 80_000),
    "F073.TCO.PRE.Z.D": (300, 2_000),
}
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# --------------------------------------------------------------------------
# MODELO (DDL)
# --------------------------------------------------------------------------
# Tablas de sql/ddl/01_modelo.sql que usa la API del BCCh. Idempotente: no borra nada.
DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;            -- lo llena el DAG de indicadores
CREATE SCHEMA IF NOT EXISTS audit;

-- RAW: respuesta tal cual llega desde la API + fecha/hora de extracción
CREATE TABLE IF NOT EXISTS raw.bcch_series (
    raw_id          BIGSERIAL PRIMARY KEY,
    serie_id        VARCHAR(60)  NOT NULL,
    fecha_desde     DATE         NOT NULL,
    fecha_hasta     DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_bcch_serie ON raw.bcch_series (serie_id, fecha_desde);

-- STAGING: datos limpios, tipados y deduplicados
CREATE TABLE IF NOT EXISTS staging.bcch_observacion (
    serie_id        VARCHAR(60)   NOT NULL,
    fecha           DATE          NOT NULL,
    valor           NUMERIC(18,4),
    es_nulo         BOOLEAN       NOT NULL DEFAULT FALSE,
    cargado_en      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (serie_id, fecha)
);

-- DW: modelo dimensional
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    tiempo_key      INTEGER PRIMARY KEY,          -- yyyymmdd
    fecha           DATE        NOT NULL UNIQUE,
    anio            SMALLINT    NOT NULL,
    trimestre       SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL,
    nombre_mes      VARCHAR(15) NOT NULL,
    dia             SMALLINT    NOT NULL,
    dia_semana      SMALLINT    NOT NULL,         -- 1=lunes ... 7=domingo
    es_fin_semana   BOOLEAN     NOT NULL,
    anio_mes        CHAR(7)     NOT NULL          -- 'YYYY-MM'
);

CREATE TABLE IF NOT EXISTS dw.dim_serie (
    serie_key       SERIAL PRIMARY KEY,
    serie_id        VARCHAR(60) NOT NULL UNIQUE,
    nombre          VARCHAR(120) NOT NULL,
    unidad          VARCHAR(20)  NOT NULL,
    moneda_codigo   VARCHAR(5),                   -- cruce con moneda de licitación
    frecuencia      VARCHAR(10)  NOT NULL
);

-- Hecho 1: valor diario de indicadores económicos (BCCh)
CREATE TABLE IF NOT EXISTS dw.fact_indicador_economico (
    tiempo_key      INTEGER NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    serie_key       INTEGER NOT NULL REFERENCES dw.dim_serie(serie_key),
    valor           NUMERIC(18,4) NOT NULL,
    variacion_diaria_pct NUMERIC(10,4),
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tiempo_key, serie_key)
);

-- AUDIT: registro de cada ejecución y de fallas
CREATE TABLE IF NOT EXISTS audit.etl_audit (
    audit_id        BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(250) NOT NULL,
    task_id         VARCHAR(250) NOT NULL,
    run_id          VARCHAR(250) NOT NULL,
    tabla_destino   VARCHAR(120),
    filas_leidas    INTEGER,
    filas_cargadas  INTEGER,
    filas_rechazadas INTEGER,
    pct_nulos       NUMERIC(7,3),
    inicio          TIMESTAMPTZ NOT NULL,
    fin             TIMESTAMPTZ,
    duracion_seg    NUMERIC(10,2),
    estado          VARCHAR(20) NOT NULL,       -- SUCCESS / FAILED / RETRY
    intento         INTEGER,
    mensaje         TEXT
);
CREATE INDEX IF NOT EXISTS ix_audit_dag ON audit.etl_audit (dag_id, inicio);
"""


def crear_modelo(engine) -> None:
    with engine.begin() as con:
        con.exec_driver_sql(DDL)
    log.info("crear_modelo: esquemas raw, staging, dw, mart y audit listos")


# --------------------------------------------------------------------------
# VALIDAR y EXTRACT
# --------------------------------------------------------------------------
def http_session(reintentos: int = 4) -> requests.Session:
    """Sesión con reintentos exponenciales ante límite de uso (HTTP 429) y errores 5xx;
    respeta la cabecera Retry-After cuando la API la envía."""
    retry = Retry(total=reintentos, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",), respect_retry_after_header=True, raise_on_status=False)
    sesion = requests.Session()
    sesion.mount("https://", HTTPAdapter(max_retries=retry))
    sesion.headers["User-Agent"] = "INF522-ETL/1.0"
    return sesion


def consultar_serie(sesion, url: str, token: str, serie: str, desde: date, hasta: date):
    """Una consulta GetSeries. La API entrega el rango completo en una sola respuesta (no pagina)."""
    time.sleep(PAUSA_ENTRE_LLAMADAS)
    resp = sesion.get(url, timeout=HTTP_TIMEOUT, params={
        "token": token, "function": "GetSeries", "timeseries": serie,
        "firstdate": desde.isoformat(), "lastdate": hasta.isoformat()})
    # 4xx/5xx que quedaron tras los reintentos. No se usa raise_for_status(): su mensaje
    # incluye la URL completa, con el token en los parámetros
    if resp.status_code >= 400:
        raise requests.HTTPError(f"HTTP {resp.status_code} en {url}: {resp.text[:200]}", response=resp)
    try:
        payload = resp.json()
    except ValueError as exc:
        raise ValueError(f"La API no devolvió JSON: {resp.text[:200]}") from exc
    # La API responde HTTP 200 también con errores de negocio (token inválido, serie inexistente)
    if payload.get("Codigo") != 0:
        raise ValueError(f"API BCCh serie {serie}: {payload.get('Codigo')} {payload.get('Descripcion')}")
    return resp.status_code, payload


def validar_api(token: str, url: str = BCCH_URL) -> None:
    """Validación previa: la API responde y el token es válido (una consulta corta)."""
    if not token:
        raise ValueError("Falta el token de la API del BCCh (Variable bcch_token o BCCH_TOKEN)")
    hoy = date.today()
    consultar_serie(http_session(reintentos=1), url, token, SERIE_VALIDACION, hoy.replace(day=1), hoy)
    log.info("validar_api: API BCCh disponible y token válido")


def extract_series(token: str, desde: date, hasta: date, url: str = BCCH_URL) -> pd.DataFrame:
    """Una llamada por serie; devuelve la respuesta completa (capa raw)."""
    filas = []
    with http_session() as sesion:
        for serie in SERIES:
            estado, payload = consultar_serie(sesion, url, token, serie, desde, hasta)
            log.info("extract_series: %-20s %s observaciones (%s a %s)",
                     serie, f"{len(_observaciones(payload)):,}", desde, hasta)
            filas.append({"serie_id": serie, "fecha_desde": desde, "fecha_hasta": hasta,
                          "http_status": estado, "payload": payload})
    return pd.DataFrame(filas)


def _observaciones(payload: dict) -> list:
    return (payload.get("Series") or {}).get("Obs") or []


def contar_observaciones(raw: pd.DataFrame) -> int:
    return sum(len(_observaciones(p)) for p in raw["payload"])


def load_raw(engine, raw: pd.DataFrame, run_id: str) -> int:
    df = raw.copy()
    df["run_id"] = run_id
    with engine.begin() as con:
        # Reejecutar el mismo run reemplaza su extracción en vez de duplicarla
        con.execute(text("DELETE FROM raw.bcch_series WHERE run_id = :run_id"), {"run_id": run_id})
        df.to_sql("bcch_series", con, schema="raw", if_exists="append", index=False, dtype={"payload": JSONB})
    log.info("  load: %-32s %s filas", "raw.bcch_series", len(df))
    return len(df)


def read_raw(engine, run_id: str) -> pd.DataFrame:
    with engine.connect() as con:
        return pd.read_sql(text("SELECT serie_id, payload FROM raw.bcch_series WHERE run_id = :run_id"),
                           con, params={"run_id": run_id})


# --------------------------------------------------------------------------
# TRANSFORM - limpieza de las observaciones
# --------------------------------------------------------------------------
def transform_observaciones(raw: pd.DataFrame) -> pd.DataFrame:
    """Una fila por serie y fecha, con el valor numérico o nulo si ese día no hubo publicación."""
    df = pd.DataFrame(
        [{"serie_id": r.serie_id, "fecha": o["indexDateString"], "valor": o.get("value"),
          "estado": o.get("statusCode", "OK")}
         for r in raw.itertuples() for o in _observaciones(r.payload)],
        columns=["serie_id", "fecha", "valor", "estado"])
    df["fecha"] = pd.to_datetime(df["fecha"], format="%d-%m-%Y")

    n0 = len(df)
    df = df.drop_duplicates(subset=["serie_id", "fecha"]).copy()
    log.info("  observaciones: %d duplicadas eliminadas", n0 - len(df))

    # "945,12" -> 945.12; "NaN", valores no positivos y estados distintos de OK -> nulo
    df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    df.loc[(df["estado"] != "OK") | (df["valor"] <= 0), "valor"] = None
    df["es_nulo"] = df["valor"].isna()
    log.info("  observaciones: %d nulas de %s (días sin publicación: fines de semana y feriados)",
             df["es_nulo"].sum(), f"{len(df):,}")
    return df[["serie_id", "fecha", "valor", "es_nulo"]].reset_index(drop=True)


def pct_nulos(obs: pd.DataFrame) -> float:
    return round(100.0 * float(obs["es_nulo"].mean()), 3) if len(obs) else 0.0


# --------------------------------------------------------------------------
# TRANSFORM - construcción de dimensiones
# --------------------------------------------------------------------------
def build_dim_tiempo(desde: date, hasta: date) -> pd.DataFrame:
    """Calendario completo de los años del rango."""
    t = pd.DataFrame({"fecha": pd.date_range(f"{desde.year}-01-01", f"{hasta.year}-12-31")})
    t["tiempo_key"] = t["fecha"].dt.strftime("%Y%m%d").astype(int)    # clave inteligente yyyymmdd
    t["anio"] = t["fecha"].dt.year
    t["trimestre"] = t["fecha"].dt.quarter
    t["mes"] = t["fecha"].dt.month
    t["nombre_mes"] = t["mes"].map(lambda m: MESES[m - 1])
    t["dia"] = t["fecha"].dt.day
    t["dia_semana"] = t["fecha"].dt.dayofweek + 1                     # 1=lunes ... 7=domingo
    t["es_fin_semana"] = t["dia_semana"] >= 6
    t["anio_mes"] = t["fecha"].dt.strftime("%Y-%m")
    return t[["tiempo_key", "fecha", "anio", "trimestre", "mes", "nombre_mes", "dia",
              "dia_semana", "es_fin_semana", "anio_mes"]]


def build_dim_serie() -> pd.DataFrame:
    """Una fila por serie; la clave subrogada serie_key la asigna PostgreSQL (SERIAL)."""
    return pd.DataFrame([{"serie_id": s, "nombre": nombre, "unidad": unidad, "moneda_codigo": moneda,
                          "frecuencia": frecuencia}
                         for s, (nombre, unidad, moneda, frecuencia) in SERIES.items()])


# --------------------------------------------------------------------------
# CALIDAD
# --------------------------------------------------------------------------
def check_calidad(obs: pd.DataFrame) -> None:
    """Validaciones antes de cargar al modelo; si alguna falla, la tarea falla y no se carga nada."""
    assert not obs.duplicated(["serie_id", "fecha"]).any(), "(serie_id, fecha) duplicado"
    assert obs["fecha"].notna().all(), "fechas inválidas"
    valores = obs[~obs["es_nulo"]]
    # Las series diarias deben traer valores en cualquier rango; la UTM solo trae el día 1 del mes
    diarias = {s for s, (*_, frecuencia) in SERIES.items() if frecuencia == "D"}
    faltantes = diarias - set(valores["serie_id"])
    assert not faltantes, f"series sin valores en el rango: {sorted(faltantes)}"
    assert (valores["valor"] > 0).all(), "valores no positivos"
    for serie, (minimo, maximo) in RANGOS_VALIDOS.items():
        fuera = valores[(valores["serie_id"] == serie) & ~valores["valor"].between(minimo, maximo)]
        assert fuera.empty, f"{SERIES[serie][0]}: {len(fuera)} valores fuera de [{minimo}, {maximo}]"
    log.info("  calidad: todas las validaciones OK")


# --------------------------------------------------------------------------
# LOAD - staging y dw con upsert (idempotente)
# --------------------------------------------------------------------------
SQL_UPSERT_STAGING = text("""
INSERT INTO staging.bcch_observacion (serie_id, fecha, valor, es_nulo)
VALUES (:serie_id, :fecha, :valor, :es_nulo)
ON CONFLICT (serie_id, fecha) DO UPDATE
SET valor = EXCLUDED.valor, es_nulo = EXCLUDED.es_nulo, cargado_en = now()
""")

SQL_INSERT_DIM_TIEMPO = text("""
INSERT INTO dw.dim_tiempo (tiempo_key, fecha, anio, trimestre, mes, nombre_mes, dia, dia_semana,
                           es_fin_semana, anio_mes)
VALUES (:tiempo_key, :fecha, :anio, :trimestre, :mes, :nombre_mes, :dia, :dia_semana,
        :es_fin_semana, :anio_mes)
ON CONFLICT (tiempo_key) DO NOTHING
""")

SQL_UPSERT_DIM_SERIE = text("""
INSERT INTO dw.dim_serie (serie_id, nombre, unidad, moneda_codigo, frecuencia)
VALUES (:serie_id, :nombre, :unidad, :moneda_codigo, :frecuencia)
ON CONFLICT (serie_id) DO UPDATE
SET nombre = EXCLUDED.nombre, unidad = EXCLUDED.unidad, moneda_codigo = EXCLUDED.moneda_codigo,
    frecuencia = EXCLUDED.frecuencia
""")

# Tabla de hechos desde staging. La variación se calcula contra el valor publicado anterior
# de la misma serie (aunque esté fuera del rango) y se descartan nulos y valores atípicos.
SQL_CARGA_FACT = text("""
WITH base AS (
    SELECT s.serie_id, s.fecha, s.valor,
           100.0 * (s.valor / NULLIF(lag(s.valor) OVER w, 0) - 1) AS var_pct
    FROM   staging.bcch_observacion s
    WHERE  NOT s.es_nulo
    WINDOW w AS (PARTITION BY s.serie_id ORDER BY s.fecha)
)
INSERT INTO dw.fact_indicador_economico (tiempo_key, serie_key, valor, variacion_diaria_pct)
SELECT to_char(b.fecha, 'YYYYMMDD')::int, d.serie_key, b.valor, round(b.var_pct::numeric, 4)
FROM   base b
JOIN   dw.dim_serie d USING (serie_id)
WHERE  b.fecha BETWEEN :desde AND :hasta
  AND  (b.var_pct IS NULL OR abs(b.var_pct) <= :umbral OR d.frecuencia = 'M')
ON CONFLICT (tiempo_key, serie_key) DO UPDATE
SET valor = EXCLUDED.valor, variacion_diaria_pct = EXCLUDED.variacion_diaria_pct, cargado_en = now()
""")


def _registros(df: pd.DataFrame) -> list:
    """DataFrame -> lista de dicts con tipos de Python (NaN -> None) para executemany."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def load_staging(engine, obs: pd.DataFrame) -> int:
    df = obs.assign(fecha=obs["fecha"].dt.date)
    with engine.begin() as con:
        con.execute(SQL_UPSERT_STAGING, _registros(df))
    log.info("  load: %-32s %s filas", "staging.bcch_observacion", f"{len(df):,}")
    return len(df)


def load_dw(engine, dim_tiempo: pd.DataFrame, dim_serie: pd.DataFrame, desde: date, hasta: date):
    """Dimensiones primero (por las FK) y luego el hecho del rango. Devuelve (leídas, cargadas)."""
    with engine.begin() as con:
        con.execute(SQL_INSERT_DIM_TIEMPO, _registros(dim_tiempo.assign(fecha=dim_tiempo["fecha"].dt.date)))
        con.execute(SQL_UPSERT_DIM_SERIE, _registros(dim_serie))
        cargadas = con.execute(SQL_CARGA_FACT, {"desde": desde, "hasta": hasta,
                                                "umbral": UMBRAL_ATIPICO_PCT}).rowcount
        leidas = con.execute(text("SELECT count(*) FROM staging.bcch_observacion "
                                  "WHERE fecha BETWEEN :desde AND :hasta"),
                             {"desde": desde, "hasta": hasta}).scalar()
    log.info("  load: %-32s %s filas (%s nulas o atípicas descartadas)",
             "dw.fact_indicador_economico", f"{cargadas:,}", leidas - cargadas)
    return leidas, cargadas


# --------------------------------------------------------------------------
# AUDITORÍA
# --------------------------------------------------------------------------
COLUMNAS_AUDITORIA = ["dag_id", "task_id", "run_id", "tabla_destino", "filas_leidas", "filas_cargadas",
                      "filas_rechazadas", "pct_nulos", "inicio", "fin", "duracion_seg", "estado",
                      "intento", "mensaje"]
CREDENCIAL_EN_URL = re.compile(r"((?:token|ticket)=)[^&\s'\"]+", re.IGNORECASE)


def ocultar_credenciales(texto):
    """Los errores de conexión de requests traen la URL con el token o ticket: se reemplaza por ***."""
    return CREDENCIAL_EN_URL.sub(r"\1***", texto) if texto else texto


def registrar_auditoria(engine, **campos) -> None:
    """Una fila en audit.etl_audit por tarea ejecutada (SUCCESS) o fallida (FAILED / RETRY)."""
    fila = {c: campos.get(c) for c in COLUMNAS_AUDITORIA}
    fila["fin"] = fila["fin"] or datetime.now(timezone.utc)
    fila["inicio"] = fila["inicio"] or fila["fin"]
    if fila["duracion_seg"] is None:
        fila["duracion_seg"] = round((fila["fin"] - fila["inicio"]).total_seconds(), 2)
    # Tipos de numpy -> Python: psycopg2 no sabe escribir np.int64 / np.float64
    for c in ["filas_leidas", "filas_cargadas", "filas_rechazadas", "intento"]:
        fila[c] = None if fila[c] is None else int(fila[c])
    fila["pct_nulos"] = None if fila["pct_nulos"] is None else float(fila["pct_nulos"])
    fila["mensaje"] = ocultar_credenciales(fila["mensaje"])
    with engine.begin() as con:
        con.execute(text(f"INSERT INTO audit.etl_audit ({', '.join(COLUMNAS_AUDITORIA)}) "
                         f"VALUES ({', '.join(':' + c for c in COLUMNAS_AUDITORIA)})"), fila)


# --------------------------------------------------------------------------
# PIPELINE
# --------------------------------------------------------------------------
def main() -> None:
    desde = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else FECHA_INICIO
    hasta = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()
    token = os.getenv("BCCH_TOKEN")
    run_id = f"manual__{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%S}"
    engine = get_engine()

    log.info("== VALIDAR ==")
    crear_modelo(engine)
    validar_api(token)

    log.info("== EXTRACT ==")
    load_raw(engine, extract_series(token, desde, hasta), run_id)

    log.info("== TRANSFORM ==")
    observaciones = transform_observaciones(read_raw(engine, run_id))

    log.info("== CALIDAD ==")
    check_calidad(observaciones)

    log.info("== LOAD ==")
    load_staging(engine, observaciones)
    load_dw(engine, build_dim_tiempo(desde, hasta), build_dim_serie(), desde, hasta)


if __name__ == "__main__":
    main()
