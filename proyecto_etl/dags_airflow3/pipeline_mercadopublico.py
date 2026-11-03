"""
PIPELINE ETL MERCADO PÚBLICO (LICITACIONES) -> MODELO DIMENSIONAL EN POSTGRESQL

Extrae las licitaciones con movimiento de cada día desde la API de Mercado Público
(ChileCompra) y las carga por capas, con las tablas de sql/ddl/01_modelo.sql:

    raw      listado diario y detalle de cada licitación, tal como llegan de la API
    staging  mp_licitacion: una fila por licitación, limpia y sin duplicados
    dw       dim_organismo, dim_estado_licitacion, dim_moneda y fact_licitacion, con el monto
             convertido a CLP y UF usando los indicadores del Banco Central (cruce entre APIs)
    audit    etl_audit

Etapas:

    1. VALIDAR    pedir el listado de ayer: la API responde y el ticket es válido
    2. EXTRACT    el listado de cada día del rango (una "página" por día) y el detalle de una
                  muestra aleatoria de licitaciones, con tope por la cuota diaria del ticket
    3. TRANSFORM  normalizar textos, convertir tipos, recodificar estados, tratar nulos y
                  atípicos, deduplicar y calcular los días entre publicación y cierre
    4. CALIDAD    validar claves, estados, montos y plazos antes de cargar al modelo
    5. LOAD       staging y dw con INSERT ... ON CONFLICT: volver a ejecutar no duplica

Documentación: https://www.chilecompra.cl/api/ (antes api.mercadopublico.cl/modules/api.aspx)
Ticket gratuito, uno por persona, 10.000 consultas diarias; se solicita con Clave Única.
Términos de uso: al publicar los datos se cita a la Dirección ChileCompra como fuente.

Datos personales: el detalle trae nombre, RUT y cargo del funcionario comprador. Por
minimización no se leen al transformar; solo quedan en el JSON crudo de raw.

La conexión, la auditoría, dim_tiempo y el modelo del BCCh (necesario para convertir los
montos) se reutilizan de pipeline_bcch.py.

Uso:  python pipeline_mercadopublico.py [fecha_desde] [fecha_hasta]      (por defecto, ayer)
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

import pipeline_bcch as bcch

log = logging.getLogger("etl")

# --- API Mercado Público -----------------------------------------------------
MP_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
PAUSA_ENTRE_LLAMADAS = 0.5         # segundos entre consultas: el ticket no admite peticiones simultáneas
MAX_DETALLE = 150                  # detalles por run; 1 run ≈ 2 + MAX_DETALLE consultas de 10.000 diarias
ERRORES_REINTENTABLES = {10500, 10501}   # peticiones simultáneas o límite de uso: esperar y reintentar

# --------------------------------------------------------------------------
# Parámetros del modelo
# --------------------------------------------------------------------------
# La documentación da 18 = Revocada y 19 = Suspendida, pero la API entrega 15 y 16 para esos
# estados (verificado con el campo Estado del detalle); se aceptan ambos
ESTADOS = {5: "Publicada", 6: "Cerrada", 7: "Desierta", 8: "Adjudicada", 15: "Revocada", 16: "Suspendida",
           18: "Revocada", 19: "Suspendida"}
ESTADOS_FINALES = (7, 8, 15, 18)   # su detalle ya no cambia: no se vuelve a descargar
PLAZO_MAXIMO_DIAS = 365            # plazos de publicación negativos o mayores son atípicos
COLUMNAS_STAGING = ["codigo_externo", "nombre", "codigo_estado", "estado", "tipo", "moneda", "monto_estimado",
                    "codigo_organismo", "nombre_organismo", "region", "comuna", "fecha_publicacion",
                    "fecha_cierre", "fecha_adjudicacion", "dias_publicacion_cierre", "cantidad_items",
                    "tiene_detalle", "fecha_consulta"]


class ApiError(Exception):
    """Error de negocio devuelto por la API (HTTP 200 con un Codigo de error en el cuerpo)."""


# --------------------------------------------------------------------------
# MODELO (DDL)
# --------------------------------------------------------------------------
# Tablas de sql/ddl/01_modelo.sql de esta API. Idempotente: no borra nada.
DDL_MP = """
-- RAW: respuesta tal cual llega desde la API + fecha/hora de extracción
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

-- STAGING: datos limpios, tipados y deduplicados
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

-- DW: dimensiones de licitaciones
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
    serie_id_bcch   VARCHAR(60)               -- serie del BCCh para convertir a CLP
);
INSERT INTO dw.dim_moneda VALUES
  ('CLP','Peso chileno',NULL),
  ('USD','Dólar estadounidense','F073.TCO.PRE.Z.D'),
  ('EUR','Euro','F072.CLP.EUR.N.O.D'),
  ('CLF','Unidad de Fomento','F073.UFF.PRE.Z.D'),
  ('UTM','Unidad Tributaria Mensual','F073.UTR.PRE.Z.M')
ON CONFLICT (moneda_codigo) DO NOTHING;

-- Hecho 2: licitaciones (Mercado Público), monto convertido a CLP y UF con BCCh
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


def crear_modelo(engine) -> None:
    """Esquemas, tablas compartidas (dim_tiempo, audit) y del BCCh, y luego las de esta API."""
    bcch.crear_modelo(engine)
    with engine.begin() as con:
        con.exec_driver_sql(DDL_MP)
    log.info("crear_modelo: tablas de Mercado Público listas")


# --------------------------------------------------------------------------
# VALIDAR y EXTRACT
# --------------------------------------------------------------------------
def consultar(sesion, url: str, params: dict, intentos: int = 4):
    """Una consulta con pausa y dos niveles de reintento: el HTTP (429/5xx) lo hace la sesión
    y el de negocio (Codigo 10500/10501, peticiones simultáneas) se hace aquí, con espera 5·i s."""
    for intento in range(1, intentos + 1):
        time.sleep(PAUSA_ENTRE_LLAMADAS)
        resp = sesion.get(url, params=params, timeout=bcch.HTTP_TIMEOUT)
        if resp.status_code >= 400:          # sin la URL completa: lleva el ticket en los parámetros
            raise requests.HTTPError(f"HTTP {resp.status_code} en {url}: {resp.text[:200]}", response=resp)
        try:
            data = resp.json()
        except ValueError as exc:
            raise ApiError(f"La API no devolvió JSON: {resp.text[:200]}") from exc
        codigo = data.get("Codigo")
        if codigo is None:                   # las respuestas correctas no traen Codigo
            return resp.status_code, data
        if codigo in ERRORES_REINTENTABLES and intento < intentos:
            log.warning("Mercado Público Codigo %s (%s); reintento en %s s", codigo, data.get("Mensaje"), 5 * intento)
            time.sleep(5 * intento)
            continue
        raise ApiError(f"API Mercado Público Codigo={codigo}: {data.get('Mensaje')}")
    raise ApiError("API Mercado Público: reintentos agotados")


def validar_api(ticket: str, url: str = MP_URL) -> None:
    """Validación previa: el listado de ayer, con un solo intento para fallar rápido sin gastar cuota."""
    if not ticket:
        raise ValueError("Falta el ticket de Mercado Público (Variable mp_ticket o MP_TICKET)")
    ayer = date.today() - timedelta(days=1)
    consultar(bcch.http_session(reintentos=1), url, {"fecha": ayer.strftime("%d%m%Y"), "ticket": ticket}, intentos=1)
    log.info("validar_api: API Mercado Público disponible y ticket válido")


def _listado(payload: dict) -> list:
    return payload.get("Listado") or []


def extract_listados(ticket: str, desde: date, hasta: date, url: str = MP_URL) -> pd.DataFrame:
    """Paginación: el listado no pagina por sí mismo, así que cada día del rango es una página."""
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
    return sum(len(_listado(p)) for p in listados["payload"])


def load_raw_listados(engine, listados: pd.DataFrame, run_id: str) -> int:
    with engine.begin() as con:
        # Reejecutar el mismo run reemplaza su extracción (listados y detalles) en vez de duplicarla
        for tabla in ["raw.mp_licitaciones_dia", "raw.mp_licitacion_detalle"]:
            con.execute(text(f"DELETE FROM {tabla} WHERE run_id = :run_id"), {"run_id": run_id})
        listados.assign(run_id=run_id).to_sql("mp_licitaciones_dia", con, schema="raw", if_exists="append",
                                              index=False, dtype={"payload": JSONB})
    log.info("  load: %-32s %s filas", "raw.mp_licitaciones_dia", len(listados))
    return len(listados)


def codigos_pendientes(engine, listados: pd.DataFrame, desde: date, hasta: date,
                       max_detalle: int = MAX_DETALLE) -> list:
    """Licitaciones a las que se les pide el detalle: las del listado, menos las que ya tienen
    detalle en un estado final, en una muestra aleatoria reproducible de hasta max_detalle."""
    codigos = {}
    for r in listados.itertuples():
        for x in _listado(r.payload):
            if x.get("CodigoExterno"):
                codigos.setdefault(x["CodigoExterno"], r.fecha_consulta)
    with engine.connect() as con:
        finales = {fila[0] for fila in con.execute(text(
            "SELECT codigo_externo FROM staging.mp_licitacion "
            "WHERE tiene_detalle AND codigo_estado = ANY(:finales)"), {"finales": list(ESTADOS_FINALES)})}
    pendientes = [(codigo, dia) for codigo, dia in codigos.items() if codigo not in finales]
    # El orden del listado no está documentado: tomar siempre los primeros podría sesgar los KPIs.
    # Misma semilla para el mismo rango = misma muestra al re-ejecutar (idempotente)
    random.Random(f"{desde}:{hasta}").shuffle(pendientes)
    log.info("codigos_pendientes: %s licitaciones sin detalle final; se piden %s",
             f"{len(pendientes):,}", min(len(pendientes), max_detalle))
    return pendientes[:max_detalle]


def extract_detalles(engine, ticket: str, pendientes: list, run_id: str, url: str = MP_URL):
    """Descarga el detalle de cada licitación pendiente a raw. Devuelve (descargados, omitidos)."""
    descargados = omitidos = 0
    with bcch.http_session() as sesion:
        for codigo, dia in pendientes:
            try:
                estado, data = consultar(sesion, url, {"codigo": codigo, "ticket": ticket})
            except ApiError as exc:
                # Error de negocio en UNA licitación: se omite, se cuenta y se sigue con las demás
                log.warning("Detalle %s omitido: %s", codigo, exc)
                omitidos += 1
                continue
            # Cada detalle en su propia transacción: si algo falla después, el avance no se pierde
            with engine.begin() as con:
                con.execute(text("INSERT INTO raw.mp_licitacion_detalle "
                                 "(codigo_externo, fecha_consulta, http_status, payload, run_id) "
                                 "VALUES (:codigo, :dia, :estado, CAST(:payload AS JSONB), :run_id)"),
                            {"codigo": codigo, "dia": dia, "estado": estado,
                             "payload": json.dumps(data), "run_id": run_id})
            descargados += 1
    log.info("extract_detalles: %d descargados y %d omitidos de %d pedidos", descargados, omitidos, len(pendientes))
    return descargados, omitidos


def read_raw(engine, run_id: str):
    """Listados y el último detalle de cada licitación extraídos en este run."""
    with engine.connect() as con:
        listados = pd.read_sql(text("SELECT fecha_consulta, payload FROM raw.mp_licitaciones_dia "
                                    "WHERE run_id = :run_id ORDER BY raw_id"), con, params={"run_id": run_id})
        detalles = pd.read_sql(text("SELECT DISTINCT ON (codigo_externo) codigo_externo, fecha_consulta, payload "
                                    "FROM raw.mp_licitacion_detalle WHERE run_id = :run_id "
                                    "ORDER BY codigo_externo, raw_id DESC"), con, params={"run_id": run_id})
    return listados, detalles


# --------------------------------------------------------------------------
# Utilidades de normalización
# --------------------------------------------------------------------------
def _texto(v):
    """Sin espacios repetidos ni saltos de línea; texto vacío -> None."""
    if v is None:
        return None
    t = " ".join(str(v).split())
    return t or None


def _clave(v):
    """Mayúsculas y sin tildes, para agrupar sin variantes ("Concepción" = "CONCEPCION")."""
    t = _texto(v)
    if t is None:
        return None
    return "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c)).upper()


def _fecha(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v)[:19])
    except ValueError:
        return None


def _monto(v):
    try:
        m = float(v)
    except (TypeError, ValueError):
        return None
    return m if m > 0 else None            # 0 o negativo = no informado


# --------------------------------------------------------------------------
# TRANSFORM
# --------------------------------------------------------------------------
def transform_licitaciones(listados: pd.DataFrame, detalles: pd.DataFrame):
    """Una fila por licitación con los datos del listado, completada con su detalle cuando se
    descargó. Devuelve (licitaciones, filas rechazadas por no traer código)."""
    registros, leidas, rechazadas = {}, 0, 0
    for r in listados.itertuples():
        for x in _listado(r.payload):
            leidas += 1
            codigo = _texto(x.get("CodigoExterno"))
            if not codigo:                     # sin código no hay clave: se rechaza
                rechazadas += 1
                continue
            # Diccionario por código = deduplicación: queda el último estado, pero la fecha de
            # movimiento es la primera vez que aparece en el rango
            previa = registros.get(codigo)
            registros[codigo] = {
                "codigo_externo": codigo, "nombre": _texto(x.get("Nombre")),
                "codigo_estado": x.get("CodigoEstado"), "fecha_cierre": _fecha(x.get("FechaCierre")),
                "fecha_consulta": min(previa["fecha_consulta"], r.fecha_consulta) if previa else r.fecha_consulta,
                "tiene_detalle": False}

    for r in detalles.itertuples():
        lista = _listado(r.payload)
        if not lista:
            continue
        x = lista[0]
        comprador, fechas = x.get("Comprador") or {}, x.get("Fechas") or {}
        fila = registros.setdefault(r.codigo_externo, {"codigo_externo": r.codigo_externo,
                                                       "fecha_consulta": r.fecha_consulta})
        publicacion = _fecha(fechas.get("FechaPublicacion"))
        cierre = _fecha(fechas.get("FechaCierre")) or fila.get("fecha_cierre")
        # Del comprador solo se leen datos del organismo, no los del funcionario (minimización)
        fila.update({
            "nombre": _texto(x.get("Nombre")),
            "codigo_estado": x.get("CodigoEstado"),
            "tipo": _texto(x.get("Tipo")),
            "moneda": (_texto(x.get("Moneda")) or "CLP").upper(),          # sin moneda = CLP
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
        })
        # Campo calculado: plazo de publicación en días; negativos o mayores al máximo son atípicos
        if publicacion and cierre:
            dias = (cierre - publicacion).days
            fila["dias_publicacion_cierre"] = dias if 0 <= dias <= PLAZO_MAXIMO_DIAS else None

    lic = pd.DataFrame(list(registros.values()), columns=COLUMNAS_STAGING)
    # Recodificación: código de estado -> descripción; los desconocidos quedan como -1
    conocido = lic["codigo_estado"].isin(list(ESTADOS))
    lic["estado"] = lic["codigo_estado"].map(ESTADOS).where(conocido, "Desconocido")
    lic["codigo_estado"] = lic["codigo_estado"].where(conocido, -1).astype(int)
    lic["tiene_detalle"] = lic["tiene_detalle"].fillna(False).astype(bool)

    log.info("  licitaciones: %s leídas, %s únicas (%s duplicadas eliminadas), %s sin código rechazadas",
             f"{leidas:,}", f"{len(lic):,}", f"{leidas - rechazadas - len(lic):,}", rechazadas)
    log.info("  licitaciones: %s con detalle; %s%% de nulos en los campos del detalle",
             f"{int(lic['tiene_detalle'].sum()):,}", pct_nulos_detalle(lic))
    return lic, rechazadas


def pct_nulos_detalle(lic: pd.DataFrame) -> float:
    """% de nulos en los campos clave del detalle, solo entre las licitaciones que lo tienen."""
    con_detalle = lic.loc[lic["tiene_detalle"], ["monto_estimado", "codigo_organismo", "fecha_publicacion", "region"]]
    return round(100.0 * float(con_detalle.isna().to_numpy().mean()), 3) if len(con_detalle) else 0.0


# --------------------------------------------------------------------------
# CALIDAD
# --------------------------------------------------------------------------
def check_calidad(lic: pd.DataFrame) -> None:
    """Validaciones antes de cargar al modelo; si alguna falla, la tarea falla y no se carga nada."""
    assert lic["codigo_externo"].notna().all(), "licitaciones sin código"
    assert lic["codigo_externo"].is_unique, "codigo_externo duplicado"
    assert lic["fecha_consulta"].notna().all(), "licitaciones sin fecha de movimiento"
    assert lic["codigo_estado"].isin(list(ESTADOS) + [-1]).all(), "estados sin recodificar"
    assert (lic["monto_estimado"].dropna() > 0).all(), "montos no positivos"
    assert lic["dias_publicacion_cierre"].dropna().between(0, PLAZO_MAXIMO_DIAS).all(), "plazos atípicos"
    assert lic.loc[lic["tiene_detalle"], "moneda"].notna().all(), "licitaciones con detalle sin moneda"
    log.info("  calidad: todas las validaciones OK")


# --------------------------------------------------------------------------
# LOAD - staging y dw con upsert (idempotente)
# --------------------------------------------------------------------------
# COALESCE: re-procesar un día que solo trae listado no borra el detalle ya cargado.
# fecha_consulta conserva la primera fecha de movimiento de la licitación
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

# Cruce con el BCCh: tipo de cambio de la moneda y valor de la UF vigentes a la fecha de
# publicación (último valor <= fecha, así los feriados sin valor usan el día hábil anterior)
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


def load_staging(engine, lic: pd.DataFrame) -> int:
    if lic.empty:
        return 0
    with engine.begin() as con:
        con.execute(SQL_UPSERT_STAGING, bcch._registros(lic))
    log.info("  load: %-32s %s filas", "staging.mp_licitacion", f"{len(lic):,}")
    return len(lic)


def load_dw(engine):
    """Dimensiones y hecho desde staging, y una validación posterior a la carga: si falla, la
    transacción se deshace y no se avisa al DAG de indicadores. Devuelve
    (leídas, cargadas, % de montos que no se pudieron convertir a CLP)."""
    with engine.begin() as con:
        primera, ultima = con.execute(text("SELECT min(LEAST(fecha_publicacion, fecha_consulta)), "
                                           "max(fecha_consulta) FROM staging.mp_licitacion")).one()
        if primera:
            dim_tiempo = bcch.build_dim_tiempo(primera, ultima)
            con.execute(bcch.SQL_INSERT_DIM_TIEMPO, bcch._registros(dim_tiempo.assign(fecha=dim_tiempo["fecha"].dt.date)))
        con.execute(SQL_UPSERT_DIM_ORGANISMO)
        cargadas = con.execute(SQL_CARGA_FACT).rowcount
        leidas = con.execute(text("SELECT count(*) FROM staging.mp_licitacion")).scalar()
        sin_estado, negativos, pct_sin_conversion = con.execute(text("""
            SELECT count(*) FILTER (WHERE codigo_estado IS NULL),
                   count(*) FILTER (WHERE monto_estimado_clp < 0),
                   round(100.0 * avg((monto_estimado_original IS NOT NULL AND monto_estimado_clp IS NULL)::int), 3)
            FROM dw.fact_licitacion""")).one()
        if sin_estado or negativos:
            raise ValueError(f"Calidad dw.fact_licitacion: {sin_estado} sin estado, {negativos} montos negativos")
    log.info("  load: %-32s %s filas (%s%% de montos sin tipo de cambio)",
             "dw.fact_licitacion", f"{cargadas:,}", pct_sin_conversion)
    return leidas, cargadas, None if pct_sin_conversion is None else float(pct_sin_conversion)


# --------------------------------------------------------------------------
# PIPELINE
# --------------------------------------------------------------------------
def main() -> None:
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
