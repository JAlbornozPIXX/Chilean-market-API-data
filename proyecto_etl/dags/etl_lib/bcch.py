#ETL API Banco Central de Chile - Base de Datos Estadísticos (SieteRestWS)
#Portal de la API (registro, activación de credenciales y documentación): https://si3.bcentral.cl/Siete/es/Siete/API
#Requiere un token de la API (registro gratuito y activación iniciando sesión en el portal).
#Se guarda como Airflow Variable `bcch_token` (inyectada desde .env como BCCH_TOKEN).

import json
import logging
import math
from datetime import date, datetime
from airflow.models import Variable
from etl_lib.common import (ApiError, Auditoria, asegurar_dim_tiempo, get_json,
                            http_session, pct_nulos, pg_cursor)

log = logging.getLogger(__name__)

url_default = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"

#! Serie_id el cual tiene el nombre, unidad, moneda que permite convertir y su frecuencia
series = {
    "F073.TCO.PRE.Z.D": ("Dólar observado", "CLP/USD", "USD", "D"),
    "F072.CLP.EUR.N.O.D": ("Euro (tipo de cambio)", "CLP/EUR", "EUR", "D"),
    "F073.UFF.PRE.Z.D": ("Unidad de Fomento", "CLP/UF", "CLF", "D"),
    "F073.UTR.PRE.Z.M": ("Unidad Tributaria Mensual", "CLP/UTM", "UTM", "M"),
}

#Umbral de variación diaria en % sobre el cual el dato se considera atípico
umbral_atipico_pct = 15.0

def config():
    return (Variable.get("bcch_base_url", default_var=url_default), Variable.get("bcch_token"))

def consulta(seccion, url, token, serie, desde, hasta):
    parametros = {"token": token, "function": "GetSeries", "timeseries": serie, "firstdate": desde.isoformat(), "lastdate": hasta.isoformat()}
    estado, data = get_json(seccion, url, parametros, min_interval=1.0)

    if(data.get("Codigo") != 0):
        raise ApiError(f"BCCh serie {serie}: Codigo = {data.get('Codigo')} {data.get('Descripcion')}")

    return estado, data

def validar_api(**contexto):
    #Para validacion previa: la api responde y el token es valido
    url, token = config()
    hoy = date.today()
    consulta(http_session(total_retries=1), url, token, "F073.UFF.PRE.Z.D", hoy.replace(day=1), hoy)
    log.info("API BCCh disponible y credenciales validas")

def rango_fechas(contexto):
    #Rango a procesar: params manuales (recarga historica) o el data_interval
    #del run, con 7 días de holgura para capturar publicaciones tardias.
    parametros = contexto["params"]
    if (parametros.get("fecha_desde") and parametros.get("fecha_hasta")):
        return date.fromisoformat(parametros["fecha_desde"]), date.fromisoformat(parametros["fecha_hasta"])
    fecha_fin = contexto["data_interval_end"].date()
    fecha_inicio = contexto["data_interval_start"].date()
    return date.fromordinal(fecha_inicio.toordinal() - 7), fecha_fin

def extraer(**contexto):
    auditoria = Auditoria(contexto, "raw.bcch_series")
    url, token = config()
    desde, hasta = rango_fechas(contexto)
    sesion = http_session()
    with pg_cursor() as cursor:
        for serie in series:
            estado_http, respuesta = consulta(sesion, url, token, serie, desde, hasta)
            observaciones = (respuesta.get("Series") or {}).get("Obs") or []
            auditoria.filas_leidas += len(observaciones)
            cursor.execute("""INSERT INTO raw.bcch_series (serie_id, fecha_desde, fecha_hasta, http_status, payload, run_id)
                            VALUES (%s, %s, %s, %s, %s, %s)""",
                            (serie, desde, hasta, estado_http, json.dumps(respuesta), contexto["run_id"]))
            log.info("Serie %s: %s observaciones (%s a %s)", serie, len(observaciones), desde, hasta)
    auditoria.filas_cargadas = auditoria.filas_leidas
    auditoria.cerrar()

def a_numero(texto): 
    #"945,12" -> float y "NaN" -> None
    if texto is None:
        return None
    texto_limpio = str(texto).strip().replace(",", ".")
    try:
        valor = float(texto_limpio)
    except ValueError:
        return None
    return None if math.isnan(valor) or valor <= 0 else valor

def transformar(**contexto):
    auditoria = Auditoria(contexto, "staging.bcch_observacion")
    filas= []
    with pg_cursor() as cursor:
        cursor.execute("""SELECT DISTINCT ON (serie_id) serie_id, payload FROM raw.bcch_series
                        WHERE run_id = %s ORDER BY serie_id, raw_id DESC""", (contexto["run_id"],))
        for serie, contenido in cursor.fetchall():
            fechas_vistas = set()
            for observacion in (contenido.get("Series") or {}).get("Obs") or []:
                fecha = datetime.strptime(observacion["indexDateString"], "%d-%m-%Y").date()
                if fecha in fechas_vistas:
                    continue
                fechas_vistas.add(fecha)
                filas.append({"serie_id": serie, "fecha": fecha, "valor": a_numero(observacion.get("value"))
                            if observacion.get("statusCode", "OK") == "OK" else None})
        auditoria.filas_leidas = len(filas)
        auditoria.pct_nulos = pct_nulos(filas, ["valor"])
        for fila in filas:
            cursor.execute(
                """INSERT INTO staging.bcch_observacion (serie_id, fecha, valor, es_nulo)
                    VALUES (%(serie_id)s, %(fecha)s, %(valor)s, %(valor)s IS NULL)
                    ON CONFLICT (serie_id, fecha) DO UPDATE
                    SET valor = EXCLUDED.valor, es_nulo = EXCLUDED.es_nulo, cargado_en = now()""", fila)
    auditoria.filas_cargadas = len(filas)
    auditoria.filas_rechazadas = sum(1 for fila in filas if fila["valor"] is None)
    auditoria.cerrar()

def cargar_dw(**contexto):
    """staging -> dw: dimensiones + hecho con variación diaria; descarta nulos
    y valores atípicos (variación > umbral_atipico_pct)."""
    auditoria = Auditoria(contexto, "dw.fact_indicador_economico")
    desde, hasta = rango_fechas(contexto)
    with pg_cursor() as cursor:
        asegurar_dim_tiempo(cursor, date(desde.year, 1, 1), date(hasta.year, 12, 31))
        for id_serie, (nombre, unidad, moneda, frecuencia) in series.items():
            cursor.execute(
                """INSERT INTO dw.dim_serie (serie_id, nombre, unidad, moneda_codigo, frecuencia)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (serie_id) DO UPDATE SET nombre=EXCLUDED.nombre, unidad=EXCLUDED.unidad""",
                (id_serie, nombre, unidad, moneda, frecuencia))
        cursor.execute(
            """
            WITH base AS (
                SELECT s.serie_id, s.fecha, s.valor,
                     100.0 * (s.valor / NULLIF(lag(s.valor) OVER w, 0) - 1) AS var_pct
                FROM staging.bcch_observacion s
                WHERE NOT s.es_nulo
                WINDOW w AS (PARTITION BY s.serie_id ORDER BY s.fecha)
            )
            INSERT INTO dw.fact_indicador_economico (tiempo_key, serie_key, valor, variacion_diaria_pct)
            SELECT to_char(b.fecha,'YYYYMMDD')::int, d.serie_key, b.valor, round(b.var_pct::numeric, 4)
            FROM base b JOIN dw.dim_serie d USING (serie_id)
            WHERE b.fecha BETWEEN %s AND %s
                AND (b.var_pct IS NULL OR abs(b.var_pct) <= %s OR d.frecuencia = 'M')
            ON CONFLICT (tiempo_key, serie_key) DO UPDATE
            SET valor = EXCLUDED.valor, variacion_diaria_pct = EXCLUDED.variacion_diaria_pct, cargado_en = now()
            """, (desde, hasta, umbral_atipico_pct))
        auditoria.filas_cargadas = cursor.rowcount
        cursor.execute("SELECT count(*) FROM staging.bcch_observacion WHERE fecha BETWEEN %s AND %s",
                        (desde, hasta))
        auditoria.filas_leidas = cursor.fetchone()[0]
        auditoria.filas_rechazadas = auditoria.filas_leidas - auditoria.filas_cargadas
    auditoria.cerrar()


def control_calidad(**contexto):
    """Validación de calidad posterior a la carga: UF y dólar deben estar
    dentro de un rango razonable."""
    with pg_cursor() as cursor:
        cursor.execute("""SELECT d.serie_id, count(*) FROM dw.fact_indicador_economico f
                            JOIN dw.dim_serie d USING (serie_key)
                            WHERE (d.serie_id='F073.UFF.PRE.Z.D' AND f.valor NOT BETWEEN 20000 AND 80000)
                            OR (d.serie_id='F073.TCO.PRE.Z.D' AND f.valor NOT BETWEEN 300 AND 2000)
                            GROUP BY 1""")
        fuera_de_rango = cursor.fetchall()
    if fuera_de_rango:
        raise ValueError(f"Valores fuera de rango en dw.fact_indicador_economico: {fuera_de_rango}")
    log.info("Control de calidad BCCh OK")