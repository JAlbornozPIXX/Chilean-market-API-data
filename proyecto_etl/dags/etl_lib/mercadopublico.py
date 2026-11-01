"""ETL API Mercado Público (ChileCompra) - Licitaciones.

Documentación: https://www.chilecompra.cl/api/
Requiere ticket (gratuito, uno por persona, 10.000 consultas diarias; se solicita con Clave Única)
"""

import json
import logging
import random
import time
import unicodedata
from datetime import date, datetime, timedelta

import requests
from airflow.models import Variable

from etl_lib.common import (ApiError, Auditoria, asegurar_dim_tiempo, get_json,
                            http_session, pct_nulos, pg_cursor)

log = logging.getLogger(__name__)

DEFAULT_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"

ESTADOS = {5: "Publicada", 6: "Cerrada", 7: "Desierta", 8: "Adjudicada",
           18: "Revocada", 19: "Suspendida"}

ERRORES_REINTENTABLES = {10500, 10501}

#configuracion y acceso a la api

def _cfg():
    #devuelve la url_base, ticket, tope_de_detalles_por_run
    return (Variable.get("mp_base_url", default_var=DEFAULT_URL),
    Variable.get("mp_ticket"),
    int(Variable.get("mp_max_detalle", default_var=150)))


def _consultar(session, url, params, intentos=4):
    #si se excede la cuota de peticiones espera y reintenta
    for i in range(1, intentos + 1):
        status, data = get_json(session, url, params, min_interval=0.5)
        codigo = data.get("Codigo")
        if codigo is None:
            return status, data
        if codigo in ERRORES_REINTENTABLES and i < intentos:
            espera = 5*i
            log.warning("Mercado Publico codigo %s (%s); reintento en %ss",
                        codigo, data.get("Mensaje"), espera)
            time.sleep(espera)
            continue
        raise ApiError(f"Mercado publico codigo= {codigo}: {data.get('Mensaje')}")
        #el bluce siempre deberia retornar o lanzar antes de llegar hasta aqui
    raise ApiError("Mercado publico: reintentos agotados")


def rango_fechas(context):
    p = context["params"]
    if p.get("fecha_desde") and p.get("fecha_hasta"):
        return date.fromisoformat(p["fecha_desde"]), date.fromisoformat(p["fecha_hasta"])
    d = context["data_interval_start"].date()
    return d, d

def validar_api(**context):
    url, ticket, _ = _cfg()
    ayer = date.today() - timedelta(days=1)
    try:

        _consultar(http_session(total_retries=1), url,
                    {"fecha": ayer.strftime("%d%m%Y"), "ticket": ticket}, intentos=1)
    except requests.RequestException as exc:
            raise ApiError(f"API Mercado Público no responde: {exc}") from exc
    log.info("API Mercado Público disponible y ticket válido")


#Extracción de la api

def extraer(**context):
    aud = Auditoria(context, "raw.mp_licitaciones_dia / raw.mp_licitacion_detalle")
    url, ticket, max_det = _cfg()
    desde, hasta = rango_fechas(context)
    s = http_session()
    codigos = []

    with pg_cursor() as cur:
        d = desde
        while d <= hasta:
            status, data = _consultar(s, url, {"fecha": d.strftime("%d%m%Y"), "ticket": ticket})
            listado = data.get("Listado") or []
            aud.filas_leidas += len(listado)
            cur.execute("""INSERT INTO raw.mp_licitaciones_dia (fecha_consulta, http_status, payload, run_id)
                           VALUES (%s,%s,%s,%s)""", (d, status, json.dumps(data), context["run_id"]))
            codigos += [(x["CodigoExterno"], d) for x in listado if x.get("CodigoExterno")]
            log.info("Listado %s: %s licitaciones", d, len(listado))
            d += timedelta(days=1)

        cur.execute("SELECT codigo_externo FROM staging.mp_licitacion WHERE tiene_detalle AND codigo_estado IN (7,8,18)")
        finales = {r[0] for r in cur.fetchall()}

    pendientes = [c for c in dict(codigos).items() if c[0] not in finales]
    #muestra aleatoria reproducible

    random.Random(f"{desde}:{hasta}").shuffle(pendientes)
    pendientes = pendientes[:max_det]

    detalles = 0
    for codigo, fconsulta in pendientes:   #lotes de detalle con tope de cuota
        try:
            status, data = _consultar(s, url, {"codigo": codigo, "ticket": ticket})
        except ApiError as exc:
            # Falla de negocio en UNA licitación: se omite, se cuenta como rechazada y se sigue
            log.warning("Detalle %s omitido: %s", codigo, exc)
            aud.filas_rechazadas += 1
            continue
        # Cada detalle se guarda en su propia transacción: el avance no se pierde si algo falla después
        with pg_cursor() as cur:
            cur.execute("""INSERT INTO raw.mp_licitacion_detalle (codigo_externo, fecha_consulta, http_status, payload, run_id)
                           VALUES (%s,%s,%s,%s,%s)""",
                        (codigo, fconsulta, status, json.dumps(data), context["run_id"]))
        detalles += 1
    aud.filas_cargadas = aud.filas_leidas
    aud.cerrar(mensaje=f"detalles descargados={detalles} de {len(pendientes)} solicitados")

#limpieza de valores

def _texto(v):
    if v is None:
        return None
    t = " ".join(str(v).split())
    return t or None


def _clave(v):
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
    return m if m > 0 else None   # 0 o negativo = no informado


#transformacion 
def transformar(**context):
    aud = Auditoria(context, "staging.mp_licitacion")
    registros = {}

    with pg_cursor() as cur:
        cur.execute("SELECT fecha_consulta, payload FROM raw.mp_licitaciones_dia WHERE run_id=%s",
                    (context["run_id"],))
        for fconsulta, payload in cur.fetchall():
            for x in payload.get("Listado") or []:
                cod = _texto(x.get("CodigoExterno"))
                if not cod:                     #sin código no hay clave: se rechaza
                    aud.filas_rechazadas += 1
                    continue
                registros[cod] = {              #dict por código = deduplicación
                    "codigo_externo": cod,
                    "nombre": _texto(x.get("Nombre")),
                    "codigo_estado": x.get("CodigoEstado"),
                    "fecha_cierre": _fecha(x.get("FechaCierre")),
                    "fecha_consulta": fconsulta,
                    "tiene_detalle": False,
                }
    
        cur.execute("""SELECT DISTINCT ON (codigo_externo) codigo_externo, fecha_consulta, payload
                       FROM raw.mp_licitacion_detalle WHERE run_id=%s
                       ORDER BY codigo_externo, raw_id DESC""", (context["run_id"],))
        for cod, fconsulta, payload in cur.fetchall():
            lst = payload.get("Listado") or []
            if not lst:
                continue
            x = lst[0]
            comp, fechas = x.get("Comprador") or {}, x.get("Fechas") or {}
            r = registros.setdefault(cod, {"codigo_externo": cod, "fecha_consulta": fconsulta})
            fpub = _fecha(fechas.get("FechaPublicacion"))
            fcie = _fecha(fechas.get("FechaCierre")) or r.get("fecha_cierre")

            r.update({
                "nombre": _texto(x.get("Nombre")),
                "codigo_estado": x.get("CodigoEstado"),
                "tipo": _texto(x.get("Tipo")),
                "moneda": (_texto(x.get("Moneda")) or "CLP").upper(),      # sin moneda = CLP
                "monto_estimado": _monto(x.get("MontoEstimado")),
                "codigo_organismo": _texto(comp.get("CodigoOrganismo")),
                "nombre_organismo": _clave(comp.get("NombreOrganismo")),
                "region": _clave(comp.get("RegionUnidad")),
                "comuna": _clave(comp.get("ComunaUnidad")),
                "fecha_publicacion": fpub.date() if fpub else None,
                "fecha_cierre": fcie,
                "fecha_adjudicacion": _fecha(fechas.get("FechaAdjudicacion")),
                "cantidad_items": (x.get("Items") or {}).get("Cantidad"),
                "tiene_detalle": True,
            })
            #campo calculado: plazo de publicación en días; negativos o > 365 son atípicos 
            if fpub and fcie:
                dias = (fcie - fpub).days
                r["dias_publicacion_cierre"] = dias if 0 <= dias <= 365 else None

        #rellena claves faltantes y recodifica el estado
        filas = []
        for r in registros.values():
            r.setdefault("tipo", None)
            r.setdefault("moneda", None)
            r.setdefault("monto_estimado", None)
            for k in ("codigo_organismo", "nombre_organismo", "region", "comuna", "fecha_publicacion",
                      "fecha_adjudicacion", "dias_publicacion_cierre", "cantidad_items", "fecha_cierre"):
                r.setdefault(k, None)
            r["estado"] = ESTADOS.get(r.get("codigo_estado"), "Desconocido")
            if r.get("codigo_estado") not in ESTADOS:
                r["codigo_estado"] = -1
            filas.append(r)

        aud.filas_leidas = len(filas)
        aud.pct_nulos = pct_nulos([f for f in filas if f["tiene_detalle"]],
                                  ["monto_estimado", "codigo_organismo", "fecha_publicacion", "region"])

        #upsert en staging; evita borrar el detalle ya cargado
        for f in filas:
            cur.execute("""
                INSERT INTO staging.mp_licitacion AS t (codigo_externo, nombre, codigo_estado, estado, tipo, moneda,
                    monto_estimado, codigo_organismo, nombre_organismo, region, comuna, fecha_publicacion,
                    fecha_cierre, fecha_adjudicacion, dias_publicacion_cierre, cantidad_items, tiene_detalle, fecha_consulta)
                VALUES (%(codigo_externo)s, %(nombre)s, %(codigo_estado)s, %(estado)s, %(tipo)s, %(moneda)s,
                    %(monto_estimado)s, %(codigo_organismo)s, %(nombre_organismo)s, %(region)s, %(comuna)s,
                    %(fecha_publicacion)s, %(fecha_cierre)s, %(fecha_adjudicacion)s, %(dias_publicacion_cierre)s,
                    %(cantidad_items)s, %(tiene_detalle)s, %(fecha_consulta)s)
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
            """, f)
    aud.filas_cargadas = len(filas)
    aud.cerrar()


#carga 
def cargar_dw(**context):
    aud = Auditoria(context, "dw.fact_licitacion")
    with pg_cursor() as cur:
        #dim_tiempo: crea las fechas que usa 
        cur.execute("""SELECT min(LEAST(fecha_publicacion, fecha_consulta)), max(fecha_consulta)
                       FROM staging.mp_licitacion""")
        mn, mx = cur.fetchone()
        if mn:
            asegurar_dim_tiempo(cur, date(mn.year, 1, 1), date(mx.year, 12, 31))

        #dim_organismo: una fila por organismo, con su version mas reciente
        cur.execute("""
            INSERT INTO dw.dim_organismo (codigo_organismo, nombre_organismo, region)
            SELECT DISTINCT ON (codigo_organismo) codigo_organismo, nombre_organismo, region
            FROM staging.mp_licitacion WHERE codigo_organismo IS NOT NULL AND nombre_organismo IS NOT NULL
            ORDER BY codigo_organismo, cargado_en DESC
            ON CONFLICT (codigo_organismo) DO UPDATE
            SET nombre_organismo = EXCLUDED.nombre_organismo, region = EXCLUDED.region, actualizado_en = now()
        """)

        #fact_licitacion: una fila por licitacion, con el monto convertido a clp y uf
        cur.execute("""
            INSERT INTO dw.fact_licitacion AS f (codigo_externo, tiempo_key, organismo_key, codigo_estado, moneda_codigo,
                tipo, monto_estimado_original, monto_estimado_clp, monto_estimado_uf, dias_publicacion_cierre, cantidad_items)
            SELECT s.codigo_externo,
                   --fecha de movimiento: dia en que la licitacion aparece en el listado
                   to_char(s.fecha_consulta,'YYYYMMDD')::int,
                   o.organismo_key, s.codigo_estado,
                   CASE WHEN m.moneda_codigo IS NOT NULL THEN s.moneda END,
                   s.tipo, s.monto_estimado,
                   --monto en clp = monto x tipo de cambio (1 si ya es clp)
                   round(s.monto_estimado * COALESCE(tc.valor, CASE WHEN s.moneda='CLP' THEN 1 END), 2),
                   --monto en uf = monto en clp / valor de la uf
                   round(s.monto_estimado * COALESCE(tc.valor, CASE WHEN s.moneda='CLP' THEN 1 END)
                         / NULLIF(uf.valor,0), 4),
                   s.dias_publicacion_cierre, s.cantidad_items
            FROM staging.mp_licitacion s
            LEFT JOIN dw.dim_organismo o USING (codigo_organismo)
            LEFT JOIN dw.dim_moneda m ON m.moneda_codigo = s.moneda
            --tipo de cambio vigente a la fecha de publicacion (ultimo valor disponible <= fecha)
            LEFT JOIN LATERAL (
                SELECT fi.valor FROM dw.fact_indicador_economico fi
                JOIN dw.dim_serie ds USING (serie_key) JOIN dw.dim_tiempo dt USING (tiempo_key)
                WHERE ds.serie_id = m.serie_id_bcch AND dt.fecha <= COALESCE(s.fecha_publicacion, s.fecha_consulta)
                ORDER BY dt.fecha DESC LIMIT 1) tc ON TRUE
            --valor de la uf vigente a la misma fecha
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
        aud.filas_cargadas = cur.rowcount       #filas del insert anterior

        cur.execute("SELECT count(*) FROM staging.mp_licitacion")
        aud.filas_leidas = cur.fetchone()[0]
        #porcentaje de montos que no se pudieron convertir a clp (falta tipo de cambio)
        cur.execute("""SELECT round(100.0*avg((monto_estimado_original IS NOT NULL AND monto_estimado_clp IS NULL)::int),3)
                       FROM dw.fact_licitacion""")
        aud.pct_nulos = cur.fetchone()[0]
    aud.cerrar()


#control de calidad: falla si hay estados nulos o montos negativos (asi no se emite el dataset)
def control_calidad(**context):
    with pg_cursor() as cur:
        cur.execute("""SELECT count(*) FILTER (WHERE codigo_estado IS NULL),
                              count(*) FILTER (WHERE monto_estimado_clp < 0)
                       FROM dw.fact_licitacion""")
        sin_estado, negativos = cur.fetchone()
    if sin_estado or negativos:
        raise ValueError(f"Calidad: {sin_estado} sin estado, {negativos} montos negativos")
    log.info("Control de calidad Mercado Público OK")
