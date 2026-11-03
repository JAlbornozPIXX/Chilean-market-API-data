"""etl de mercado publico (licitaciones de chilecompra) hacia el modelo dimensional en postgresql.

capas: raw (listado diario y detalle tal cual), staging (una fila por licitacion, limpia),
dw (dimensiones y fact_licitacion con el monto en clp y uf usando el bcch) y audit.

etapas: validar -> extract -> transform -> calidad -> load. cada etapa es una funcion;
main() las ejecuta en orden y dag_pipeline_mercadopublico.py convierte cada una en una tarea.

api: https://www.chilecompra.cl/api/ (ticket gratuito por persona, 10.000 consultas diarias).
al publicar los datos se cita a la direccion chilecompra como fuente.

datos personales: el detalle trae nombre, rut y cargo del funcionario comprador; no se leen
al transformar y solo quedan en el json crudo de raw (minimizacion).

la conexion, la auditoria, dim_tiempo y el modelo del bcch se reutilizan de pipeline_bcch.py.

uso: python pipeline_mercadopublico.py [fecha_desde] [fecha_hasta]   (por defecto, ayer)
     con MP_TICKET y PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD como variables de entorno
"""

import json
import logging
import os
import random
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine

import pipeline_bcch as bcch

log = logging.getLogger("etl")

# api de mercado publico
MP_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
PAUSA_ENTRE_LLAMADAS = 0.5  # segundos entre consultas: el ticket no admite peticiones simultaneas
MAX_DETALLE = 150  # detalles por run; cada run usa 2 + MAX_DETALLE de las 10.000 consultas diarias
ERRORES_REINTENTABLES = {10500, 10501}  # peticiones simultaneas o limite de uso: esperar y reintentar

# --------------------------------------------------------------------------
# parametros del modelo
# --------------------------------------------------------------------------
# la documentacion dice 18 = revocada y 19 = suspendida, pero la api entrega 15 y 16
# (verificado con el campo estado del detalle); se aceptan ambos
ESTADOS = {
    5: "Publicada", 6: "Cerrada", 7: "Desierta", 8: "Adjudicada",
    15: "Revocada", 16: "Suspendida", 18: "Revocada", 19: "Suspendida",
}  # fmt: skip
ESTADOS_FINALES = (7, 8, 15, 18)  # su detalle ya no cambia: no se vuelve a descargar
PLAZO_MAXIMO_DIAS = 365  # plazos negativos o mayores se consideran atipicos
COLUMNAS_STAGING = [
    "codigo_externo", "nombre", "codigo_estado", "estado", "tipo", "moneda", "monto_estimado",
    "codigo_organismo", "nombre_organismo", "region", "comuna", "fecha_publicacion", "fecha_cierre",
    "fecha_adjudicacion", "dias_publicacion_cierre", "cantidad_items", "tiene_detalle", "fecha_consulta",
]  # fmt: skip


class ApiError(Exception):
    """error de negocio de la api (http 200 con un codigo de error en el cuerpo)."""


# --------------------------------------------------------------------------
# modelo (ddl)
# --------------------------------------------------------------------------
# tablas de sql/ddl/01_modelo.sql de esta api; idempotente, no borra nada
DDL_MP = """
-- raw: respuesta tal cual llega desde la api + fecha y hora de extraccion
CREATE TABLE IF NOT EXISTS raw.mp_licitaciones_dia (
    raw_id          BIGSERIAL PRIMARY KEY,
    fecha_consulta  DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_mp_dia ON raw.mp_licitaciones_dia (fecha_consulta);

CREATE TABLE IF NOT EXISTS raw.mp_licitacion_detalle (
    raw_id          BIGSERIAL PRIMARY KEY,
    codigo_externo  VARCHAR(40)  NOT NULL,
    fecha_consulta  DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_mp_det ON raw.mp_licitacion_detalle (codigo_externo);

-- staging: datos limpios, tipados y sin duplicados
CREATE TABLE IF NOT EXISTS staging.mp_licitacion (
    codigo_externo        VARCHAR(40) PRIMARY KEY,
    nombre                TEXT,
    codigo_estado         INTEGER,
    estado                VARCHAR(40),
    tipo                  VARCHAR(10),
    moneda                VARCHAR(5),
    monto_estimado        NUMERIC(20,2),
    codigo_organismo      VARCHAR(20),
    nombre_organismo      TEXT,
    region                VARCHAR(80),
    comuna                VARCHAR(80),
    fecha_publicacion     DATE,
    fecha_cierre          TIMESTAMP,
    fecha_adjudicacion    TIMESTAMP,
    dias_publicacion_cierre INTEGER,
    cantidad_items        INTEGER,
    tiene_detalle         BOOLEAN NOT NULL DEFAULT FALSE,
    fecha_consulta        DATE NOT NULL,
    cargado_en            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- dw: dimensiones de licitaciones
CREATE TABLE IF NOT EXISTS dw.dim_organismo (
    organismo_key   SERIAL PRIMARY KEY,
    codigo_organismo VARCHAR(20) NOT NULL UNIQUE,
    nombre_organismo TEXT        NOT NULL,
    region          VARCHAR(80),
    actualizado_en  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_estado_licitacion (
    codigo_estado   INTEGER PRIMARY KEY,
    estado          VARCHAR(40) NOT NULL,
    es_final        BOOLEAN     NOT NULL,
    es_exitoso      BOOLEAN     NOT NULL
);
INSERT INTO dw.dim_estado_licitacion VALUES
  (5,'Publicada',FALSE,FALSE),(6,'Cerrada',FALSE,FALSE),(7,'Desierta',TRUE,FALSE),
  (8,'Adjudicada',TRUE,TRUE),(15,'Revocada',TRUE,FALSE),(16,'Suspendida',FALSE,FALSE),
  (18,'Revocada',TRUE,FALSE),(19,'Suspendida',FALSE,FALSE),(-1,'Desconocido',FALSE,FALSE)
ON CONFLICT (codigo_estado) DO NOTHING;

CREATE TABLE IF NOT EXISTS dw.dim_moneda (
    moneda_codigo   VARCHAR(5) PRIMARY KEY,
    descripcion     VARCHAR(40) NOT NULL,
    serie_id_bcch   VARCHAR(60)               -- serie del bcch para convertir a clp
);
INSERT INTO dw.dim_moneda VALUES
  ('CLP','Peso chileno',NULL),
  ('USD','Dólar estadounidense','F073.TCO.PRE.Z.D'),
  ('EUR','Euro','F072.CLP.EUR.N.O.D'),
  ('CLF','Unidad de Fomento','F073.UFF.PRE.Z.D'),
  ('UTM','Unidad Tributaria Mensual','F073.UTR.PRE.Z.M')
ON CONFLICT (moneda_codigo) DO NOTHING;

-- hecho 2: licitaciones, con el monto convertido a clp y uf usando el bcch
CREATE TABLE IF NOT EXISTS dw.fact_licitacion (
    codigo_externo  VARCHAR(40) PRIMARY KEY,
    tiempo_key      INTEGER NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    organismo_key   INTEGER REFERENCES dw.dim_organismo(organismo_key),
    codigo_estado   INTEGER NOT NULL REFERENCES dw.dim_estado_licitacion(codigo_estado),
    moneda_codigo   VARCHAR(5) REFERENCES dw.dim_moneda(moneda_codigo),
    tipo            VARCHAR(10),
    monto_estimado_original NUMERIC(20,2),
    monto_estimado_clp      NUMERIC(22,2),
    monto_estimado_uf       NUMERIC(18,4),
    dias_publicacion_cierre INTEGER,
    cantidad_items  INTEGER,
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_lic_tiempo ON dw.fact_licitacion (tiempo_key);
"""


def crear_modelo(engine: Engine) -> None:
    """crea las tablas compartidas y del bcch, y luego las de esta api."""
    bcch.crear_modelo(engine)
    with engine.begin() as con:
        con.exec_driver_sql(DDL_MP)
    log.info("crear_modelo: tablas de Mercado Publico listas")


# --------------------------------------------------------------------------
# validar y extract
# --------------------------------------------------------------------------
def consultar(sesion: requests.Session, url: str, params: dict, intentos: int = 4) -> tuple[int, dict]:
    """una consulta con pausa y dos niveles de reintento.

    el reintento http (429 y 5xx) lo hace la sesion; el de negocio (codigos 10500 y 10501,
    peticiones simultaneas) se hace aqui esperando 5 * intento segundos.
    """
    for intento in range(1, intentos + 1):
        time.sleep(PAUSA_ENTRE_LLAMADAS)
        resp = sesion.get(url, params=params, timeout=bcch.HTTP_TIMEOUT)
        if resp.status_code >= 400:  # sin la url completa: lleva el ticket en los parametros
            raise requests.HTTPError(f"HTTP {resp.status_code} en {url}: {resp.text[:200]}", response=resp)
        try:
            data = resp.json()
        except ValueError as exc:
            raise ApiError(f"La API no devolvio JSON: {resp.text[:200]}") from exc
        codigo = data.get("Codigo")
        if codigo is None:  # las respuestas correctas no traen codigo
            return resp.status_code, data
        if codigo in ERRORES_REINTENTABLES and intento < intentos:
            log.warning("Mercado Publico Codigo %s (%s); reintento en %s s", codigo, data.get("Mensaje"), 5 * intento)
            time.sleep(5 * intento)
            continue
        raise ApiError(f"API Mercado Publico Codigo={codigo}: {data.get('Mensaje')}")
    raise ApiError("API Mercado Publico: reintentos agotados")


def validar_api(ticket: str, url: str = MP_URL) -> None:
    """pide el listado de ayer con un solo intento, para fallar rapido sin gastar cuota."""
    if not ticket:
        raise ValueError("Falta el ticket de Mercado Publico (Variable mp_ticket o MP_TICKET)")
    ayer = date.today() - timedelta(days=1)
    params = {"fecha": ayer.strftime("%d%m%Y"), "ticket": ticket}
    consultar(bcch.http_session(reintentos=1), url, params, intentos=1)
    log.info("validar_api: API Mercado Publico disponible y ticket valido")


def _listado(payload: dict) -> list:
    """lista de licitaciones de una respuesta (vacia si no trae)."""
    return payload.get("Listado") or []


def extract_listados(ticket: str, desde: date, hasta: date, url: str = MP_URL) -> pd.DataFrame:
    """paginacion: el listado no pagina por si mismo, asi que cada dia del rango es una pagina."""
    filas = []
    with bcch.http_session() as sesion:
        dia = desde
        while dia <= hasta:
            estado, data = consultar(sesion, url, {"fecha": dia.strftime("%d%m%Y"), "ticket": ticket})
            log.info("extract_listados: %s  %s licitaciones", dia, f"{len(_listado(data)):,}")
            filas.append({"fecha_consulta": dia, "http_status": estado, "payload": data})
            dia += timedelta(days=1)
    return pd.DataFrame(filas, columns=["fecha_consulta", "http_status", "payload"])


def contar_licitaciones(listados: pd.DataFrame) -> int:
    """total de licitaciones en los listados extraidos."""
    return sum(len(_listado(p)) for p in listados["payload"])


def load_raw_listados(engine: Engine, listados: pd.DataFrame, run_id: str) -> int:
    """guarda los listados en raw; devuelve las filas guardadas."""
    with engine.begin() as con:
        # reejecutar el mismo run reemplaza su extraccion (listados y detalles) en vez de duplicarla
        for tabla in ["raw.mp_licitaciones_dia", "raw.mp_licitacion_detalle"]:
            con.execute(text(f"DELETE FROM {tabla} WHERE run_id = :run_id"), {"run_id": run_id})
        listados.assign(run_id=run_id).to_sql(
            "mp_licitaciones_dia", con, schema="raw", if_exists="append", index=False, dtype={"payload": JSONB}
        )
    log.info("  load: %-32s %s filas", "raw.mp_licitaciones_dia", len(listados))
    return len(listados)


def codigos_pendientes(
    engine: Engine, listados: pd.DataFrame, desde: date, hasta: date, max_detalle: int = MAX_DETALLE
) -> list[tuple[str, date]]:
    """licitaciones del listado sin detalle final, en una muestra aleatoria de hasta max_detalle."""
    codigos = {}
    for r in listados.itertuples():
        for x in _listado(r.payload):
            if x.get("CodigoExterno"):
                codigos.setdefault(x["CodigoExterno"], r.fecha_consulta)
    with engine.connect() as con:
        consulta = text(
            "SELECT codigo_externo FROM staging.mp_licitacion WHERE tiene_detalle AND codigo_estado = ANY(:finales)"
        )
        finales = {fila[0] for fila in con.execute(consulta, {"finales": list(ESTADOS_FINALES)})}
    pendientes = [(codigo, dia) for codigo, dia in codigos.items() if codigo not in finales]
    # el orden del listado no esta documentado: tomar siempre los primeros podria sesgar los kpis.
    # la misma semilla para el mismo rango da la misma muestra al reejecutar (idempotente)
    random.Random(f"{desde}:{hasta}").shuffle(pendientes)
    log.info(
        "codigos_pendientes: %s licitaciones sin detalle final; se piden %s",
        f"{len(pendientes):,}", min(len(pendientes), max_detalle),
    )  # fmt: skip
    return pendientes[:max_detalle]


def extract_detalles(
    engine: Engine, ticket: str, pendientes: list[tuple[str, date]], run_id: str, url: str = MP_URL
) -> tuple[int, int]:
    """descarga a raw el detalle de cada licitacion pendiente; devuelve (descargados, omitidos)."""
    descargados = omitidos = 0
    insertar = text(
        "INSERT INTO raw.mp_licitacion_detalle "
        "(codigo_externo, fecha_consulta, http_status, payload, run_id) "
        "VALUES (:codigo, :dia, :estado, CAST(:payload AS JSONB), :run_id)"
    )
    with bcch.http_session() as sesion:
        for codigo, dia in pendientes:
            try:
                estado, data = consultar(sesion, url, {"codigo": codigo, "ticket": ticket})
            except ApiError as exc:
                # error de negocio en una licitacion: se omite, se cuenta y se sigue con las demas
                log.warning("Detalle %s omitido: %s", codigo, exc)
                omitidos += 1
                continue
            # cada detalle en su propia transaccion: si algo falla despues, el avance no se pierde
            with engine.begin() as con:
                fila = {"codigo": codigo, "dia": dia, "estado": estado, "payload": json.dumps(data), "run_id": run_id}
                con.execute(insertar, fila)
            descargados += 1
    log.info("extract_detalles: %d descargados y %d omitidos de %d pedidos", descargados, omitidos, len(pendientes))
    return descargados, omitidos


def read_raw(engine: Engine, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """listados y el ultimo detalle de cada licitacion extraidos en este run."""
    with engine.connect() as con:
        listados = pd.read_sql(
            text("SELECT fecha_consulta, payload FROM raw.mp_licitaciones_dia WHERE run_id = :run_id ORDER BY raw_id"),
            con,
            params={"run_id": run_id},
        )
        detalles = pd.read_sql(
            text(
                "SELECT DISTINCT ON (codigo_externo) codigo_externo, fecha_consulta, payload "
                "FROM raw.mp_licitacion_detalle WHERE run_id = :run_id "
                "ORDER BY codigo_externo, raw_id DESC"
            ),
            con,
            params={"run_id": run_id},
        )
    return listados, detalles


# --------------------------------------------------------------------------
# utilidades de normalizacion
# --------------------------------------------------------------------------
def _texto(v: object) -> str | None:
    """texto sin espacios repetidos ni saltos de linea; vacio -> none."""
    if v is None:
        return None
    t = " ".join(str(v).split())
    return t or None


def _clave(v: object) -> str | None:
    """texto en mayusculas y sin tildes, para agrupar sin variantes (concepcion = CONCEPCION)."""
    t = _texto(v)
    if t is None:
        return None
    return "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c)).upper()


def _fecha(v: object) -> datetime | None:
    """fecha iso de la api a datetime; vacia o invalida -> none."""
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v)[:19])
    except ValueError:
        return None


def _monto(v: object) -> float | None:
    """monto numerico; 0, negativo o invalido se toma como no informado."""
    try:
        m = float(v)
    except (TypeError, ValueError):
        return None
    return m if m > 0 else None


# --------------------------------------------------------------------------
# transform
# --------------------------------------------------------------------------
def transform_licitaciones(listados: pd.DataFrame, detalles: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """una fila por licitacion con el listado, completada con su detalle cuando se descargo.

    devuelve (licitaciones, filas rechazadas por no traer codigo).
    """
    registros, leidas, rechazadas = {}, 0, 0
    for r in listados.itertuples():
        for x in _listado(r.payload):
            leidas += 1
            codigo = _texto(x.get("CodigoExterno"))
            if not codigo:  # sin codigo no hay clave: se rechaza
                rechazadas += 1
                continue
            # el diccionario por codigo deduplica: queda el ultimo estado, pero la fecha de
            # movimiento es la primera vez que la licitacion aparece en el rango
            previa = registros.get(codigo)
            registros[codigo] = {
                "codigo_externo": codigo,
                "nombre": _texto(x.get("Nombre")),
                "codigo_estado": x.get("CodigoEstado"),
                "fecha_cierre": _fecha(x.get("FechaCierre")),
                "fecha_consulta": min(previa["fecha_consulta"], r.fecha_consulta) if previa else r.fecha_consulta,
                "tiene_detalle": False,
            }

    for r in detalles.itertuples():
        lista = _listado(r.payload)
        if not lista:
            continue
        x = lista[0]
        comprador, fechas = x.get("Comprador") or {}, x.get("Fechas") or {}
        fila = registros.setdefault(
            r.codigo_externo, {"codigo_externo": r.codigo_externo, "fecha_consulta": r.fecha_consulta}
        )
        publicacion = _fecha(fechas.get("FechaPublicacion"))
        cierre = _fecha(fechas.get("FechaCierre")) or fila.get("fecha_cierre")
        # del comprador solo se leen datos del organismo, no los del funcionario (minimizacion)
        fila.update(
            {
                "nombre": _texto(x.get("Nombre")),
                "codigo_estado": x.get("CodigoEstado"),
                "tipo": _texto(x.get("Tipo")),
                "moneda": (_texto(x.get("Moneda")) or "CLP").upper(),  # sin moneda = clp
                "monto_estimado": _monto(x.get("MontoEstimado")),
                "codigo_organismo": _texto(comprador.get("CodigoOrganismo")),
                "nombre_organismo": _clave(comprador.get("NombreOrganismo")),
                "region": _clave(comprador.get("RegionUnidad")),
                "comuna": _clave(comprador.get("ComunaUnidad")),
                "fecha_publicacion": publicacion.date() if publicacion else None,
                "fecha_cierre": cierre,
                "fecha_adjudicacion": _fecha(fechas.get("FechaAdjudicacion")),
                "cantidad_items": (x.get("Items") or {}).get("Cantidad"),
                "tiene_detalle": True,
            }
        )
        # campo calculado: plazo de publicacion en dias; negativos o mayores al maximo son atipicos
        if publicacion and cierre:
            dias = (cierre - publicacion).days
            fila["dias_publicacion_cierre"] = dias if 0 <= dias <= PLAZO_MAXIMO_DIAS else None

    lic = pd.DataFrame(list(registros.values()), columns=COLUMNAS_STAGING)
    # recodificacion: codigo de estado -> descripcion; los desconocidos quedan como -1
    conocido = lic["codigo_estado"].isin(list(ESTADOS))
    lic["estado"] = lic["codigo_estado"].map(ESTADOS).where(conocido, "Desconocido")
    lic["codigo_estado"] = lic["codigo_estado"].where(conocido, -1).astype(int)
    lic["tiene_detalle"] = lic["tiene_detalle"].fillna(False).astype(bool)

    duplicadas = leidas - rechazadas - len(lic)
    log.info(
        "  licitaciones: %s leidas, %s unicas (%s duplicadas eliminadas), %s sin codigo rechazadas",
        f"{leidas:,}", f"{len(lic):,}", f"{duplicadas:,}", rechazadas,
    )  # fmt: skip
    log.info(
        "  licitaciones: %s con detalle; %s%% de nulos en los campos del detalle",
        f"{int(lic['tiene_detalle'].sum()):,}", pct_nulos_detalle(lic),
    )  # fmt: skip
    return lic, rechazadas


def pct_nulos_detalle(lic: pd.DataFrame) -> float:
    """porcentaje de nulos en los campos clave del detalle, solo en las licitaciones que lo tienen."""
    campos = ["monto_estimado", "codigo_organismo", "fecha_publicacion", "region"]
    con_detalle = lic.loc[lic["tiene_detalle"], campos]
    return round(100.0 * float(con_detalle.isna().to_numpy().mean()), 3) if len(con_detalle) else 0.0


# --------------------------------------------------------------------------
# calidad
# --------------------------------------------------------------------------
def check_calidad(lic: pd.DataFrame) -> None:
    """validaciones antes de cargar; si una falla, la tarea falla y no se carga nada."""
    assert lic["codigo_externo"].notna().all(), "licitaciones sin codigo"
    assert lic["codigo_externo"].is_unique, "codigo_externo duplicado"
    assert lic["fecha_consulta"].notna().all(), "licitaciones sin fecha de movimiento"
    assert lic["codigo_estado"].isin(list(ESTADOS) + [-1]).all(), "estados sin recodificar"
    assert (lic["monto_estimado"].dropna() > 0).all(), "montos no positivos"
    assert lic["dias_publicacion_cierre"].dropna().between(0, PLAZO_MAXIMO_DIAS).all(), "plazos atipicos"
    assert lic.loc[lic["tiene_detalle"], "moneda"].notna().all(), "licitaciones con detalle sin moneda"
    log.info("  calidad: todas las validaciones OK")


# --------------------------------------------------------------------------
# load: staging y dw con upsert (idempotente)
# --------------------------------------------------------------------------
# coalesce: reprocesar un dia que solo trae listado no borra el detalle ya cargado;
# fecha_consulta conserva la primera fecha de movimiento de la licitacion
SQL_UPSERT_STAGING = text("""
INSERT INTO staging.mp_licitacion AS t (codigo_externo, nombre, codigo_estado, estado, tipo, moneda,
    monto_estimado, codigo_organismo, nombre_organismo, region, comuna, fecha_publicacion,
    fecha_cierre, fecha_adjudicacion, dias_publicacion_cierre, cantidad_items, tiene_detalle, fecha_consulta)
VALUES (:codigo_externo, :nombre, :codigo_estado, :estado, :tipo, :moneda,
    :monto_estimado, :codigo_organismo, :nombre_organismo, :region, :comuna, :fecha_publicacion,
    :fecha_cierre, :fecha_adjudicacion, :dias_publicacion_cierre, :cantidad_items, :tiene_detalle, :fecha_consulta)
ON CONFLICT (codigo_externo) DO UPDATE SET
    nombre = EXCLUDED.nombre, codigo_estado = EXCLUDED.codigo_estado, estado = EXCLUDED.estado,
    tipo = COALESCE(EXCLUDED.tipo, t.tipo), moneda = COALESCE(EXCLUDED.moneda, t.moneda),
    monto_estimado = COALESCE(EXCLUDED.monto_estimado, t.monto_estimado),
    codigo_organismo = COALESCE(EXCLUDED.codigo_organismo, t.codigo_organismo),
    nombre_organismo = COALESCE(EXCLUDED.nombre_organismo, t.nombre_organismo),
    region = COALESCE(EXCLUDED.region, t.region), comuna = COALESCE(EXCLUDED.comuna, t.comuna),
    fecha_publicacion = COALESCE(EXCLUDED.fecha_publicacion, t.fecha_publicacion),
    fecha_cierre = COALESCE(EXCLUDED.fecha_cierre, t.fecha_cierre),
    fecha_adjudicacion = COALESCE(EXCLUDED.fecha_adjudicacion, t.fecha_adjudicacion),
    dias_publicacion_cierre = COALESCE(EXCLUDED.dias_publicacion_cierre, t.dias_publicacion_cierre),
    cantidad_items = COALESCE(EXCLUDED.cantidad_items, t.cantidad_items),
    tiene_detalle = t.tiene_detalle OR EXCLUDED.tiene_detalle,
    fecha_consulta = LEAST(t.fecha_consulta, EXCLUDED.fecha_consulta), cargado_en = now()
""")

SQL_UPSERT_DIM_ORGANISMO = text("""
INSERT INTO dw.dim_organismo (codigo_organismo, nombre_organismo, region)
SELECT DISTINCT ON (codigo_organismo) codigo_organismo, nombre_organismo, region
FROM   staging.mp_licitacion
WHERE  codigo_organismo IS NOT NULL AND nombre_organismo IS NOT NULL
ORDER BY codigo_organismo, cargado_en DESC
ON CONFLICT (codigo_organismo) DO UPDATE
SET nombre_organismo = EXCLUDED.nombre_organismo, region = EXCLUDED.region, actualizado_en = now()
""")

# cruce con el bcch: tipo de cambio de la moneda y uf vigentes a la fecha de publicacion
# (ultimo valor <= fecha, asi los feriados sin valor usan el dia habil anterior)
SQL_CARGA_FACT = text("""
INSERT INTO dw.fact_licitacion AS f (codigo_externo, tiempo_key, organismo_key, codigo_estado, moneda_codigo,
    tipo, monto_estimado_original, monto_estimado_clp, monto_estimado_uf, dias_publicacion_cierre, cantidad_items)
SELECT s.codigo_externo,
       to_char(s.fecha_consulta, 'YYYYMMDD')::int,        -- fecha de movimiento (aparece en el listado)
       o.organismo_key, s.codigo_estado,
       CASE WHEN m.moneda_codigo IS NOT NULL THEN s.moneda END,
       s.tipo, s.monto_estimado,
       round(s.monto_estimado * COALESCE(tc.valor, CASE WHEN s.moneda = 'CLP' THEN 1 END), 2),
       round(s.monto_estimado * COALESCE(tc.valor, CASE WHEN s.moneda = 'CLP' THEN 1 END) / NULLIF(uf.valor, 0), 4),
       s.dias_publicacion_cierre, s.cantidad_items
FROM   staging.mp_licitacion s
LEFT JOIN dw.dim_organismo o USING (codigo_organismo)
LEFT JOIN dw.dim_moneda m ON m.moneda_codigo = s.moneda
LEFT JOIN LATERAL (
    SELECT fi.valor FROM dw.fact_indicador_economico fi
    JOIN dw.dim_serie ds USING (serie_key) JOIN dw.dim_tiempo dt USING (tiempo_key)
    WHERE ds.serie_id = m.serie_id_bcch AND dt.fecha <= COALESCE(s.fecha_publicacion, s.fecha_consulta)
    ORDER BY dt.fecha DESC LIMIT 1) tc ON TRUE
LEFT JOIN LATERAL (
    SELECT fi.valor FROM dw.fact_indicador_economico fi
    JOIN dw.dim_serie ds USING (serie_key) JOIN dw.dim_tiempo dt USING (tiempo_key)
    WHERE ds.serie_id = 'F073.UFF.PRE.Z.D' AND dt.fecha <= COALESCE(s.fecha_publicacion, s.fecha_consulta)
    ORDER BY dt.fecha DESC LIMIT 1) uf ON TRUE
ON CONFLICT (codigo_externo) DO UPDATE SET
    tiempo_key = EXCLUDED.tiempo_key, organismo_key = EXCLUDED.organismo_key,
    codigo_estado = EXCLUDED.codigo_estado, moneda_codigo = EXCLUDED.moneda_codigo, tipo = EXCLUDED.tipo,
    monto_estimado_original = EXCLUDED.monto_estimado_original,
    monto_estimado_clp = EXCLUDED.monto_estimado_clp, monto_estimado_uf = EXCLUDED.monto_estimado_uf,
    dias_publicacion_cierre = EXCLUDED.dias_publicacion_cierre,
    cantidad_items = EXCLUDED.cantidad_items, cargado_en = now()
""")

# validacion posterior a la carga del hecho
SQL_CALIDAD_DW = text("""
SELECT count(*) FILTER (WHERE codigo_estado IS NULL),
       count(*) FILTER (WHERE monto_estimado_clp < 0),
       round(100.0 * avg((monto_estimado_original IS NOT NULL AND monto_estimado_clp IS NULL)::int), 3)
FROM dw.fact_licitacion
""")


def load_staging(engine: Engine, lic: pd.DataFrame) -> int:
    """upsert de las licitaciones en staging; devuelve las filas cargadas."""
    if lic.empty:
        return 0
    with engine.begin() as con:
        con.execute(SQL_UPSERT_STAGING, bcch._registros(lic))
    log.info("  load: %-32s %s filas", "staging.mp_licitacion", f"{len(lic):,}")
    return len(lic)


def load_dw(engine: Engine) -> tuple[int, int, float | None]:
    """dimensiones y hecho desde staging, con una validacion posterior a la carga.

    si la validacion falla, la transaccion se deshace y no se avisa al dag de indicadores.
    devuelve (leidas, cargadas, porcentaje de montos que no se pudieron convertir a clp).
    """
    with engine.begin() as con:
        primera, ultima = con.execute(
            text("SELECT min(LEAST(fecha_publicacion, fecha_consulta)), max(fecha_consulta) FROM staging.mp_licitacion")
        ).one()
        if primera:
            dim_tiempo = bcch.build_dim_tiempo(primera, ultima)
            filas_tiempo = bcch._registros(dim_tiempo.assign(fecha=dim_tiempo["fecha"].dt.date))
            con.execute(bcch.SQL_INSERT_DIM_TIEMPO, filas_tiempo)
        con.execute(SQL_UPSERT_DIM_ORGANISMO)
        cargadas = con.execute(SQL_CARGA_FACT).rowcount
        leidas = con.execute(text("SELECT count(*) FROM staging.mp_licitacion")).scalar()
        sin_estado, negativos, pct_sin_conversion = con.execute(SQL_CALIDAD_DW).one()
        if sin_estado or negativos:
            raise ValueError(f"Calidad dw.fact_licitacion: {sin_estado} sin estado, {negativos} montos negativos")
    log.info(
        "  load: %-32s %s filas (%s%% de montos sin tipo de cambio)",
        "dw.fact_licitacion", f"{cargadas:,}", pct_sin_conversion,
    )  # fmt: skip
    return leidas, cargadas, None if pct_sin_conversion is None else float(pct_sin_conversion)


# --------------------------------------------------------------------------
# pipeline completo (sin airflow)
# --------------------------------------------------------------------------
def main() -> None:
    """ejecuta todas las etapas en orden para el rango de fechas recibido."""
    ayer = date.today() - timedelta(days=1)
    desde = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else ayer
    hasta = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else desde
    ticket = os.getenv("MP_TICKET")
    run_id = f"manual__{datetime.now():%Y-%m-%dT%H:%M:%S}"
    engine = bcch.get_engine()

    log.info("== VALIDAR ==")
    crear_modelo(engine)
    validar_api(ticket)

    log.info("== EXTRACT ==")
    listados = extract_listados(ticket, desde, hasta)
    load_raw_listados(engine, listados, run_id)
    extract_detalles(engine, ticket, codigos_pendientes(engine, listados, desde, hasta), run_id)

    log.info("== TRANSFORM ==")
    licitaciones, _ = transform_licitaciones(*read_raw(engine, run_id))

    log.info("== CALIDAD ==")
    check_calidad(licitaciones)

    log.info("== LOAD ==")
    load_staging(engine, licitaciones)
    load_dw(engine)


if __name__ == "__main__":
    main()
