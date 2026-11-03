"""
PIPELINE DE INDICADORES DE GESTIÓN -> ESQUEMA MART EN POSTGRESQL

Pregunta de gestión: ¿qué tan eficiente, competitivo y transparente es el proceso de
licitaciones del Estado y cuánto se licita en términos reales?

Calcula 6 indicadores mensuales con SQL sobre el modelo dw (Mercado Público + Banco Central):

    KPI1  Tasa de adjudicación (%)                       meta >= 80   eficiencia
    KPI2  Tasa de licitaciones desiertas (%)             meta <= 15   eficiencia
    KPI3  Plazo promedio publicación-cierre (días)       meta >= 10   competencia
    KPI4  Monto estimado licitado (miles de UF)          informativo  volumen real (cruce con BCCh)
    KPI5  Licitaciones con monto estimado visible (%)    meta >= 50   transparencia
    KPI6  Concentración top-10 organismos en monto (%)   meta <= 60   competencia

Etapas:

    1. MODELO     asegurar las tablas de dw (de los dos ETL) y de mart
    2. FRESCURA   revisar la última carga de cada fuente; solo advierte, no detiene el cálculo
    3. KPIs       recálculo completo e idempotente (DELETE + INSERT en una transacción)

Cada etapa es una función con entradas y salidas explícitas; main() las ejecuta en orden
y dag_pipeline_kpis.py convierte cada una en una tarea de Airflow.

Uso:  python pipeline_kpis.py      (con PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
"""
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

import pipeline_bcch as bcch
import pipeline_mercadopublico as mp

log = logging.getLogger("etl")

DIAS_FRESCURA = 3                  # una fuente sin cargas en más días se considera desactualizada

# --------------------------------------------------------------------------
# MODELO (DDL)
# --------------------------------------------------------------------------
# Tablas de mart de sql/ddl/01_modelo.sql. Idempotente: no borra nada.
DDL_MART = """
CREATE TABLE IF NOT EXISTS mart.kpi_mensual (
    anio_mes        CHAR(7)      NOT NULL,
    kpi_codigo      VARCHAR(20)  NOT NULL,
    kpi_nombre      VARCHAR(120) NOT NULL,
    valor           NUMERIC(22,4),
    meta            NUMERIC(22,4),
    cumple_meta     BOOLEAN,
    calculado_en    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (anio_mes, kpi_codigo)
);

CREATE TABLE IF NOT EXISTS mart.top_organismos_mes (
    anio_mes        CHAR(7) NOT NULL,
    ranking         SMALLINT NOT NULL,
    nombre_organismo TEXT NOT NULL,
    monto_clp       NUMERIC(22,2),
    participacion_pct NUMERIC(8,4),
    PRIMARY KEY (anio_mes, ranking)
);
"""


def crear_modelo(engine) -> None:
    """Tablas de los dos ETL (los KPIs pueden correr aunque una fuente nunca haya cargado) y de mart."""
    mp.crear_modelo(engine)
    with engine.begin() as con:
        con.exec_driver_sql(DDL_MART)
    log.info("crear_modelo: tablas de mart listas")


# --------------------------------------------------------------------------
# FRESCURA
# --------------------------------------------------------------------------
def verificar_frescura(engine) -> dict:
    """Última carga de cada tabla de hechos. No bloquea: si una fuente falló, los KPIs se
    recalculan igual con la otra fuente y con el último dato válido de la caída."""
    with engine.connect() as con:
        ultimas = dict(con.execute(text(
            "SELECT 'bcch', max(cargado_en) FROM dw.fact_indicador_economico "
            "UNION ALL SELECT 'mercadopublico', max(cargado_en) FROM dw.fact_licitacion")).all())
    limite = datetime.now(timezone.utc) - timedelta(days=DIAS_FRESCURA)
    for fuente, ultima in ultimas.items():
        if ultima is None or ultima < limite:
            log.warning("verificar_frescura: %s desactualizada (última carga %s); KPIs con el último dato disponible",
                        fuente, ultima)
        else:
            log.info("verificar_frescura: %s cargada por última vez el %s", fuente, ultima)
    return ultimas


# --------------------------------------------------------------------------
# KPIs (copiado de sql/kpi/kpis.sql)
# --------------------------------------------------------------------------
SQL_KPIS = """
DELETE FROM mart.kpi_mensual;
DELETE FROM mart.top_organismos_mes;

WITH lic AS (
    SELECT t.anio_mes, f.*, e.es_final, e.es_exitoso
    FROM dw.fact_licitacion f
    JOIN dw.dim_tiempo t USING (tiempo_key)
    JOIN dw.dim_estado_licitacion e USING (codigo_estado)
),
k AS (
    -- KPI1 Tasa de adjudicación (sobre licitaciones con estado final)
    SELECT anio_mes, 'KPI1' cod, 'Tasa de adjudicación (%)' nom,
           100.0 * count(*) FILTER (WHERE es_exitoso) / NULLIF(count(*) FILTER (WHERE es_final),0) val,
           80.0 meta, 'mayor' sentido
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI2 Tasa de licitaciones desiertas
    SELECT anio_mes, 'KPI2', 'Tasa de licitaciones desiertas (%)',
           100.0 * count(*) FILTER (WHERE codigo_estado = 7) / NULLIF(count(*) FILTER (WHERE es_final),0),
           15.0, 'menor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI3 Plazo promedio de publicación (días entre publicación y cierre)
    SELECT anio_mes, 'KPI3', 'Plazo promedio publicación-cierre (días)',
           avg(dias_publicacion_cierre), 10.0, 'mayor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI4 Monto licitado en términos reales (miles de UF) - cruce con BCCh
    SELECT anio_mes, 'KPI4', 'Monto estimado licitado (miles de UF)',
           sum(monto_estimado_uf) / 1000.0, NULL, 'info'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI5 Transparencia: % de licitaciones con monto estimado publicado (sobre las que tienen detalle)
    SELECT anio_mes, 'KPI5', 'Licitaciones con monto estimado visible (%)',
           100.0 * count(monto_estimado_original) / NULLIF(count(*) FILTER (WHERE cantidad_items IS NOT NULL),0),
           50.0, 'mayor'
    FROM lic GROUP BY anio_mes
)
INSERT INTO mart.kpi_mensual (anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta)
SELECT anio_mes, cod, nom, round(val::numeric, 4), meta,
       CASE WHEN val IS NULL OR meta IS NULL THEN NULL
            WHEN sentido = 'mayor' THEN val >= meta ELSE val <= meta END
FROM k;

-- KPI6 Concentración: participación de los 10 organismos con mayor monto (CLP)
WITH m AS (
    SELECT t.anio_mes, o.nombre_organismo, sum(f.monto_estimado_clp) monto
    FROM dw.fact_licitacion f
    JOIN dw.dim_tiempo t USING (tiempo_key)
    JOIN dw.dim_organismo o USING (organismo_key)
    WHERE f.monto_estimado_clp IS NOT NULL
    GROUP BY 1, 2
), r AS (
    SELECT *, row_number() OVER (PARTITION BY anio_mes ORDER BY monto DESC) rk,
           100.0 * monto / sum(monto) OVER (PARTITION BY anio_mes) pct
    FROM m
)
INSERT INTO mart.top_organismos_mes
SELECT anio_mes, rk, nombre_organismo, monto, round(pct::numeric, 4) FROM r WHERE rk <= 10;

INSERT INTO mart.kpi_mensual (anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta)
SELECT anio_mes, 'KPI6', 'Concentración top-10 organismos en monto (%)',
       round(sum(participacion_pct), 4), 60.0, sum(participacion_pct) <= 60.0
FROM mart.top_organismos_mes GROUP BY anio_mes;

-- Vista de apoyo para el informe: contexto económico del mes (dólar, euro, UF, UTM)
CREATE OR REPLACE VIEW mart.v_contexto_economico_mes AS
SELECT t.anio_mes, s.nombre serie, round(avg(f.valor), 2) valor_promedio,
       round(stddev(f.variacion_diaria_pct), 4) volatilidad_diaria_pct
FROM dw.fact_indicador_economico f
JOIN dw.dim_tiempo t USING (tiempo_key)
JOIN dw.dim_serie s USING (serie_key)
GROUP BY 1, 2;
"""


def calcular_kpis(engine):
    """Recalcula todo mart en una transacción: si algo falla, quedan los KPIs anteriores.
    Devuelve (filas de kpi_mensual, filas de top_organismos_mes, % de KPIs sin valor)."""
    with engine.begin() as con:
        # Cursor de psycopg2 directo: el SQL trae varias sentencias y textos con "%"
        con.connection.cursor().execute(SQL_KPIS)
        n_kpi, n_top, pct_nulos = con.execute(text(
            "SELECT (SELECT count(*) FROM mart.kpi_mensual), (SELECT count(*) FROM mart.top_organismos_mes), "
            "(SELECT round(100.0 * avg((valor IS NULL)::int), 3) FROM mart.kpi_mensual)")).one()
    log.info("calcular_kpis: %s filas en mart.kpi_mensual y %s en mart.top_organismos_mes (%s%% de KPIs sin valor)",
             n_kpi, n_top, pct_nulos)
    return n_kpi, n_top, None if pct_nulos is None else float(pct_nulos)


def resumen_kpis(engine) -> pd.DataFrame:
    """Tabla para el informe: un KPI por fila y un mes por columna."""
    with engine.connect() as con:
        k = pd.read_sql(text("SELECT anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta "
                             "FROM mart.kpi_mensual ORDER BY kpi_codigo, anio_mes"), con)
    k["valor"] = k["valor"].astype(float).round(2)
    return k.pivot_table(index=["kpi_codigo", "kpi_nombre"], columns="anio_mes", values="valor")


# --------------------------------------------------------------------------
# PIPELINE
# --------------------------------------------------------------------------
def main() -> None:
    engine = bcch.get_engine()

    log.info("== MODELO ==")
    crear_modelo(engine)

    log.info("== FRESCURA ==")
    verificar_frescura(engine)

    log.info("== KPIs ==")
    calcular_kpis(engine)

    pd.set_option("display.width", 200)
    print("\nIndicadores de gestión por mes:\n")
    print(resumen_kpis(engine).to_string())


if __name__ == "__main__":
    main()
