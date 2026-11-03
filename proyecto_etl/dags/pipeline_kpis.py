"""indicadores de gestion de compras publicas hacia el esquema mart en postgresql.

pregunta de gestion: que tan eficiente, competitivo y transparente es el proceso de
licitaciones del estado y cuanto se licita en terminos reales.

calcula 6 indicadores mensuales con sql sobre el modelo dw (mercado publico + banco central):

    kpi1  tasa de adjudicacion (%)                       meta >= 80   eficiencia
    kpi2  tasa de licitaciones desiertas (%)             meta <= 15   eficiencia
    kpi3  plazo promedio publicacion-cierre (dias)       meta >= 10   competencia
    kpi4  monto estimado licitado (miles de uf)          umbral +-20% respecto del mes anterior
    kpi5  licitaciones con monto estimado visible (%)    meta >= 50   transparencia
    kpi6  concentracion top-10 organismos en monto (%)   meta <= 60   competencia

etapas: modelo -> frescura (solo advierte) -> kpis (recalculo completo e idempotente).
main() las ejecuta en orden y dag_pipeline_kpis.py convierte cada una en una tarea.

uso: python pipeline_kpis.py   (con PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
"""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

import pipeline_bcch as bcch
import pipeline_mercadopublico as mp

log = logging.getLogger("etl")

DIAS_FRESCURA = 3  # una fuente sin cargas en mas dias se considera desactualizada

# --------------------------------------------------------------------------
# modelo (ddl)
# --------------------------------------------------------------------------
# tablas de mart de sql/ddl/01_modelo.sql; idempotente, no borra nada
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


def crear_modelo(engine: Engine) -> None:
    """tablas de los dos etl (los kpis corren aunque una fuente nunca haya cargado) y de mart."""
    mp.crear_modelo(engine)
    with engine.begin() as con:
        con.exec_driver_sql(DDL_MART)
    log.info("crear_modelo: tablas de mart listas")


# --------------------------------------------------------------------------
# frescura
# --------------------------------------------------------------------------
def verificar_frescura(engine: Engine) -> dict[str, datetime | None]:
    """ultima carga de cada tabla de hechos; no bloquea el calculo si una fuente fallo."""
    with engine.connect() as con:
        consulta = text(
            "SELECT 'bcch', max(cargado_en) FROM dw.fact_indicador_economico "
            "UNION ALL SELECT 'mercadopublico', max(cargado_en) FROM dw.fact_licitacion"
        )
        ultimas = dict(con.execute(consulta).all())
    limite = datetime.now(timezone.utc) - timedelta(days=DIAS_FRESCURA)
    for fuente, ultima in ultimas.items():
        if ultima is None or ultima < limite:
            log.warning(
                "verificar_frescura: %s desactualizada (ultima carga %s); KPIs con el ultimo dato disponible",
                fuente, ultima,
            )  # fmt: skip
        else:
            log.info("verificar_frescura: %s cargada por ultima vez el %s", fuente, ultima)
    return ultimas


# --------------------------------------------------------------------------
# kpis (copiado de sql/kpi/kpis.sql)
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
    -- kpi1 tasa de adjudicacion (sobre licitaciones con estado final)
    SELECT anio_mes, 'KPI1' cod, 'Tasa de adjudicación (%)' nom,
           100.0 * count(*) FILTER (WHERE es_exitoso) / NULLIF(count(*) FILTER (WHERE es_final),0) val,
           80.0 meta, 'mayor' sentido
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- kpi2 tasa de licitaciones desiertas
    SELECT anio_mes, 'KPI2', 'Tasa de licitaciones desiertas (%)',
           100.0 * count(*) FILTER (WHERE codigo_estado = 7) / NULLIF(count(*) FILTER (WHERE es_final),0),
           15.0, 'menor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- kpi3 plazo promedio de publicacion (dias entre publicacion y cierre)
    SELECT anio_mes, 'KPI3', 'Plazo promedio publicación-cierre (días)',
           avg(dias_publicacion_cierre), 10.0, 'mayor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- kpi4 monto licitado en terminos reales (miles de uf), cruce con el bcch
    SELECT anio_mes, 'KPI4', 'Monto estimado licitado (miles de UF)',
           sum(monto_estimado_uf) / 1000.0, NULL, 'info'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- kpi5 transparencia: % de licitaciones con monto publicado (sobre las que tienen detalle)
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

-- kpi6 concentracion: participacion de los 10 organismos con mayor monto (clp)
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

-- vista de apoyo para el informe: contexto economico del mes (dolar, euro, uf, utm)
CREATE OR REPLACE VIEW mart.v_contexto_economico_mes AS
SELECT t.anio_mes, s.nombre serie, round(avg(f.valor), 2) valor_promedio,
       round(stddev(f.variacion_diaria_pct), 4) volatilidad_diaria_pct
FROM dw.fact_indicador_economico f
JOIN dw.dim_tiempo t USING (tiempo_key)
JOIN dw.dim_serie s USING (serie_key)
GROUP BY 1, 2;
"""

# conteos que se registran en la auditoria despues del calculo
SQL_CONTEOS = text("""
SELECT (SELECT count(*) FROM mart.kpi_mensual),
       (SELECT count(*) FROM mart.top_organismos_mes),
       (SELECT round(100.0 * avg((valor IS NULL)::int), 3) FROM mart.kpi_mensual)
""")


def calcular_kpis(engine: Engine) -> tuple[int, int, float | None]:
    """recalcula todo mart en una transaccion; si algo falla quedan los kpis anteriores.

    devuelve (filas de kpi_mensual, filas de top_organismos_mes, porcentaje de kpis sin valor).
    """
    with engine.begin() as con:
        # cursor de psycopg2 directo: el sql trae varias sentencias y textos con "%"
        con.connection.cursor().execute(SQL_KPIS)
        n_kpi, n_top, pct_nulos = con.execute(SQL_CONTEOS).one()
    log.info(
        "calcular_kpis: %s filas en mart.kpi_mensual y %s en mart.top_organismos_mes (%s%% de KPIs sin valor)",
        n_kpi, n_top, pct_nulos,
    )  # fmt: skip
    return n_kpi, n_top, None if pct_nulos is None else float(pct_nulos)


def resumen_kpis(engine: Engine) -> pd.DataFrame:
    """tabla para revisar los resultados: un kpi por fila y un mes por columna."""
    with engine.connect() as con:
        consulta = text(
            "SELECT anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta "
            "FROM mart.kpi_mensual ORDER BY kpi_codigo, anio_mes"
        )
        k = pd.read_sql(consulta, con)
    k["valor"] = k["valor"].astype(float).round(2)
    return k.pivot_table(index=["kpi_codigo", "kpi_nombre"], columns="anio_mes", values="valor")


# --------------------------------------------------------------------------
# pipeline completo (sin airflow)
# --------------------------------------------------------------------------
def main() -> None:
    """ejecuta las etapas en orden y muestra los kpis por mes."""
    engine = bcch.get_engine()

    log.info("== MODELO ==")
    crear_modelo(engine)

    log.info("== FRESCURA ==")
    verificar_frescura(engine)

    log.info("== KPIs ==")
    calcular_kpis(engine)

    pd.set_option("display.width", 200)
    print("\nIndicadores de gestion por mes:\n")
    print(resumen_kpis(engine).to_string())


if __name__ == "__main__":
    main()
