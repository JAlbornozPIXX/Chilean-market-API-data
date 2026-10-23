"""Aplica al generador de entregables del proyecto de referencia las mejoras de la Issue 5.

Uso, desde la raíz del repositorio y DESPUÉS de copiar el archivo original:
    cp referencia/proyecto_etl/docs/generar_entregables.py proyecto_etl/docs/
    python3 planificacion/recursos/aplicar_mejoras_informe.py proyecto_etl/docs/generar_entregables.py

Es "todo o nada": si algún fragmento original no aparece exactamente una vez (por ejemplo,
porque ya aplicaste las mejoras o el archivo no es el de referencia), no modifica nada y
te dice qué fragmento falló. Cada cambio está explicado en la Issue 5 (tabla del Paso 8).
"""
import sys
from pathlib import Path

if len(sys.argv) != 2:
    sys.exit(__doc__)
ruta = Path(sys.argv[1])
fuente = ruta.read_text(encoding="utf-8")


def bloque(desde, hasta):
    """Texto del archivo original entre dos marcas ('desde' incluido, 'hasta' excluido)."""
    i = fuente.find(desde)
    j = fuente.find(hasta, i + 1) if i >= 0 else -1
    return fuente[i:j] if 0 <= i < j else None


CAMBIOS = [
    ("01 Constantes INTEGRANTES (nombres reales) y SQL_DIR",
     r'''FIG = BASE / "figuras"
OUT.mkdir(exist_ok=True); FIG.mkdir(exist_ok=True)
''',
     r'''FIG = BASE / "figuras"
OUT.mkdir(exist_ok=True); FIG.mkdir(exist_ok=True)
SQL_DIR = BASE.parent / "sql"
# Nombres tal como deben aparecer en la portada del informe y en la lámina 1
INTEGRANTES = ["Nombre Apellido 1", "Nombre Apellido 2", "Nombre Apellido 3", "Nombre Apellido 4", "Nombre Apellido 5"]
'''),

    ("02 Conexión a airflow_meta y q() con conexión opcional",
     r'''                        dbname=os.getenv("PGDATABASE", "dw_gestion"))


def q(sql):
    with conn.cursor() as c:
''',
     r'''                        dbname=os.getenv("PGDATABASE", "dw_gestion"))
# BD de metadatos de Airflow (mismo servidor): duración real de cada DAG run
conn_meta = psycopg2.connect(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5433"),
                             user=os.getenv("PGUSER", "etl"), password=os.environ["PGPASSWORD"],
                             dbname=os.getenv("PGDATABASE_META", "airflow_meta"))


def q(sql, cn=None):
    with (cn or conn).cursor() as c:
'''),

    ("03 Tiempos con 2 decimales y duración real de cada DAG run (tabla dag_run)",
     r'''        round(avg(dur),1) "dur. prom (s)", round(max(dur),1) "dur. máx (s)"
 FROM (SELECT dag_id, run_id, sum(duracion_seg) dur FROM audit.etl_audit
       WHERE estado='SUCCESS' GROUP BY 1,2) x GROUP BY 1 ORDER BY 1""")
''',
     r'''        round(avg(dur),2) "dur. prom (s)", round(max(dur),2) "dur. máx (s)"
 FROM (SELECT dag_id, run_id, sum(duracion_seg) dur FROM audit.etl_audit
       WHERE estado='SUCCESS' GROUP BY 1,2) x GROUP BY 1 ORDER BY 1""")
TIEMPOS_AF = q("""
 SELECT dag_id, count(*) runs,
        round(avg(extract(epoch FROM end_date - start_date))::numeric, 1) "dur. prom (s)",
        round(max(extract(epoch FROM end_date - start_date))::numeric, 1) "dur. máx (s)"
 FROM dag_run WHERE state = 'success' AND end_date IS NOT NULL
 GROUP BY dag_id ORDER BY dag_id""", conn_meta)
'''),

    ("04 Top-10: encabezado 'organismo' en vez de 'left'",
     r'''TOP = q("""SELECT ranking, left(nombre_organismo,55), round(monto_clp/1e6,1) "MM CLP",''',
     r'''TOP = q("""SELECT ranking, left(nombre_organismo,55) organismo, round(monto_clp/1e6,1) "MM CLP",'''),

    ("05 Métricas de calidad medidas, KPIs que cumplen / no cumplen y conteo de HTTP 429",
     r'''MES = KPI[1][0][0] if KPI[1] else "—"
''',
     r'''MES = KPI[1][0][0] if KPI[1] else "—"
CUMPLEN = [r[1] for r in KPI[1] if r[5] == "Sí"]
NO_CUMPLEN = [r[1] for r in KPI[1] if r[5] == "No"]
CAL = q("""SELECT
   (SELECT round(100.0*avg((monto_estimado IS NULL)::int),1) FROM staging.mp_licitacion WHERE tiene_detalle),
   (SELECT round(100.0*avg((monto_estimado_original IS NOT NULL AND monto_estimado_uf IS NULL)::int),1) FROM dw.fact_licitacion),
   (SELECT count(*) FILTER (WHERE es_nulo) FROM staging.bcch_observacion),
   (SELECT count(*) FROM staging.bcch_observacion)""")[1][0]
N429 = q("SELECT count(*) FROM audit.etl_audit WHERE mensaje LIKE '%429%'")[1][0][0]
'''),

    ("06 Figura de arquitectura sin texto fuera de las cajas",
     bloque('f3 = FIG / "arquitectura.png"\n', 'f4 = FIG / "dag_tareas.png"\n'),
     r'''f3 = FIG / "arquitectura.png"
fig, ax = plt.subplots(figsize=(10, 3.6)); ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
W, H = 0.16, 0.24   # todas las cajas del mismo tamaño y sin superponerse
cajas = [(0.01, 0.62, "API Banco Central\n(SieteRestWS)"), (0.01, 0.14, "API Mercado Público\n(licitaciones.json)"),
         (0.21, 0.62, "DAG\netl_bcch_indicadores"), (0.21, 0.14, "DAG\netl_mercadopublico_\nlicitaciones"),
         (0.41, 0.38, "PostgreSQL\nraw → staging → dw"), (0.61, 0.38, "DAG kpis_gestion_\ncompras_publicas\n(Dataset BCCH | MP)"),
         (0.81, 0.38, "mart\n(KPIs)")]
for x0, y0, t in cajas:
    ax.add_patch(plt.Rectangle((x0, y0), W, H, fc="#e3f2fd", ec="#1565c0"))
    ax.text(x0 + W / 2, y0 + H / 2, t, ha="center", va="center", fontsize=8)
for a, b in [((0.17, 0.74), (0.21, 0.74)), ((0.17, 0.26), (0.21, 0.26)), ((0.37, 0.74), (0.41, 0.56)),
             ((0.37, 0.26), (0.41, 0.44)), ((0.57, 0.50), (0.61, 0.50)), ((0.77, 0.50), (0.81, 0.50))]:
    ax.annotate("", xy=b, xytext=a, arrowprops=dict(arrowstyle="->", color="#1565c0"))
ax.text(0.41, 0.92, "audit.etl_audit registra cada tarea (éxito / reintento / falla)", fontsize=8, color="#555")
fig.tight_layout(); fig.savefig(f3, dpi=160); plt.close(fig)
'''),

    ("07 Portada con los integrantes reales",
     r'''tabla(["Campo", "Detalle"], [["Integrantes", "[Nombre 1] / [Nombre 2] / [Nombre 3]"],''',
     r'''tabla(["Campo", "Detalle"], [["Integrantes", "<br/>".join(INTEGRANTES)],'''),

    ("08 Límites de Mercado Público según los términos de uso de ChileCompra",
     r'''["Límites", "Cuota diaria por ticket (≈10.000 solicitudes); error 10500 por peticiones simultáneas; HTTP 429",''',
     r'''["Límites", "10.000 solicitudes diarias por ticket (un ticket por persona); descargas masivas recomendadas entre 22:00 y 07:00; error 10500 por peticiones simultáneas; HTTP 429",'''),

    ("09 Diagrama del modelo dimensional en la sección 3.3",
     r'''dim_organismo, dim_estado_licitacion, dim_moneda, dim_serie."]),
      Paragraph("<b>Idempotencia:</b>''',
     r'''dim_organismo, dim_estado_licitacion, dim_moneda, dim_serie."]),
      img(FIG / "modelo_dimensional.png", 15),
      Paragraph("<b>Idempotencia:</b>'''),

    ("10 Leyenda de la captura del BCCh (ahora con ejecuciones exitosas)",
     r'''("03_grid_bcch.png", "Grilla del DAG del BCCh (falla aislada con reintentos)"),''',
     r'''("03_grid_bcch.png", "Grilla del DAG del BCCh (ejecuciones diarias y recarga histórica exitosas)"),'''),

    ("11 Leyenda de la captura de recuperación (sin fecha fija)",
     r'''("07_grid_recuperado.png", "Recuperación tras la falla controlada (Clear del run 08-10)")]:''',
     r'''("07_grid_recuperado.png", "Falla controlada: reintentos, falla y recuperación con Clear")]:'''),

    ("12 KPI4 con umbral de alerta (la pauta pide meta o umbral para cada KPI)",
     r'''"MP + BCCh", "Mensual", "Seguimiento (sin meta; se compara con mes anterior)"],''',
     r'''"MP + BCCh", "Mensual", "Umbral de alerta: variación > ±20 % vs. mes anterior"],'''),

    ("13 Tabla de reglas de calidad con resultados medidos (6.2)",
     r'''si falla, no se emite el Dataset y los KPIs no se recalculan con datos malos."]),
      Paragraph("6.3 Origen y linaje del dato", H2),''',
     r'''si falla, no se emite el Dataset y los KPIs no se recalculan con datos malos."]),
      Paragraph("<b>Reglas de calidad y resultado medido en la carga final</b>", SM),
      tabla(["Regla de calidad", "Dónde se aplica", "Resultado medido"], [
          ["Monto estimado no publicado (nulo)", "transformar_staging (MP); se mide con KPI5", f"{fmt(CAL[0])} % de las licitaciones con detalle"],
          ["Licitaciones sin detalle por la cuota del ticket", "extraer_raw (MP), Variable mp_max_detalle", f"{fmt(SIN_DET)} % del total"],
          ["Montos sin conversión a UF (falta valor BCCh)", "cargar_dw (MP), JOIN LATERAL con BCCh", f"{fmt(CAL[1])} % de las licitaciones"],
          ["Observaciones del BCCh nulas (feriados, NaN)", "transformar_staging (BCCh), columna es_nulo", f"{fmt(CAL[2])} de {fmt(CAL[3])} observaciones"],
          ["Claves duplicadas en dw", "PRIMARY KEY + INSERT … ON CONFLICT", "0 (lo garantiza la clave primaria)"],
          ["UF fuera de 20.000–80.000 o dólar fuera de 300–2.000", "control_calidad (BCCh)", "0 (si aparece, la tarea falla)"],
          ["Estados nulos o montos negativos", "control_calidad (MP)", "0 (si aparece, la tarea falla)"]],
            [5.5 * cm, 5.5 * cm, 5 * cm]),
      Paragraph("6.3 Origen y linaje del dato", H2),'''),

    ("14 Matriz de riesgos con probabilidad, impacto, mitigación y evidencia (6.5)",
     bloque('      Paragraph("6.5 Riesgos", H2), *bullets([', '      Paragraph("6.6 Escalabilidad", H2)'),
     r'''      Paragraph("6.5 Riesgos", H2), Paragraph(
          "La matriz resume cada riesgo, su probabilidad e impacto estimados, la mitigación ya implementada "
          "en la solución y dónde se puede verificar:", P),
      tabla(["Riesgo", "Prob.", "Impacto", "Mitigación implementada", "Evidencia"], [
          ["Cambio de formato o versión de una API", "Media", "Alto", "JSON crudo en raw para re-procesar sin volver a llamar; validar_api y control_calidad fallan explícitamente", "raw.*.payload; ApiError en los logs"],
          ["Límites de uso: 10.000 solicitudes/día por ticket, error 10500, HTTP 429", "Alta", "Medio", "max_active_runs=1, pausas entre llamadas, Retry con backoff, tope mp_max_detalle, cargas masivas de noche", "Filas RETRY en etl_audit; mensaje «detalles descargados»"],
          ["Caída temporal de una API", "Media", "Medio", "Reintentos con espera, DAGs independientes, KPIs con Dataset OR y aviso de frescura", "Demostración de falla; tabla de fallas (sección 7)"],
          ["Exposición de credenciales", "Baja", "Alto", "Connection y Variables desde .env (no se versiona ni se entrega); .gitignore", "Carpeta .zip sin .env"],
          ["Licencias y uso de los datos", "Baja", "Medio", "Datos abiertos; se cita a ChileCompra y al Banco Central como fuente", "Informe y presentación"],
          ["Datos personales (nombre, RUT y cargo del funcionario)", "Media", "Alto", "Minimización (Ley 19.628/21.719): no pasan a staging ni dw; rol analista_kpi sin acceso a raw", "transformar(); DDL"],
          ["Sesgo por detalle parcial (cuota)", "Alta", "Medio", "Muestra aleatoria reproducible; más detalle en horario nocturno; KPI3 a KPI6 se informan como estimación", "% sin detalle (sección 6.2)"]],
            [3.4 * cm, 1.3 * cm, 1.7 * cm, 5.6 * cm, 4 * cm]),
'''),

    ("15 Nueva sección 6.7 Gobierno, seguridad y operación",
     r'''nuevos KPIs (por región, rubro, proveedor) se agregan como nuevas consultas sobre el mismo modelo."]),
      PageBreak()]''',
     r'''nuevos KPIs (por región, rubro, proveedor) se agregan como nuevas consultas sobre el mismo modelo."]),
      Paragraph("6.7 Gobierno, seguridad y operación", H2), *bullets([
          "<b>Accesos:</b> solo el usuario técnico del ETL escribe en la base; el rol de solo lectura <i>analista_kpi</i> consulta dw y mart, pero no raw, donde queda el JSON original con datos personales.",
          "<b>Retención:</b> raw conserva cada extracción para trazabilidad y re-proceso; se propone eliminar el detalle crudo con más de 90 días mediante un DAG de mantención, porque contiene datos personales.",
          "<b>Monitoreo:</b> cada tarea queda en audit.etl_audit y cada falla o reintento deja una línea [ALERTA] en el log; el siguiente paso es enviar esas alertas por correo desde on_failure_callback.",
          "<b>Documentación y reproducibilidad:</b> el DDL incluye un diccionario de datos (COMMENT ON) y el README permite levantar la solución desde cero con docker compose; la configuración vive fuera del código."]),
      PageBreak()]'''),

    ("16 Sección 7: duración real por DAG (Airflow) además del procesamiento (auditoría)",
     r'''      Paragraph("<b>Tiempo de ejecución por DAG</b> (suma de duraciones de tareas por run)", SM), tabla(TIEMPOS[0], TIEMPOS[1]), Spacer(1, 6),
''',
     r'''      Paragraph("<b>Tiempo de ejecución por DAG</b> (duración real de cada run según Airflow, tabla dag_run)", SM), tabla(TIEMPOS_AF[0], TIEMPOS_AF[1]), Spacer(1, 6),
      Paragraph("<b>Tiempo de procesamiento por DAG</b> (suma de duraciones de tareas por run, audit.etl_audit)", SM), tabla(TIEMPOS[0], TIEMPOS[1]), Spacer(1, 6),
'''),

    ("17 Ancho de la columna 'intento' en la tabla de fallas",
     r'''tabla(FALLAS[0], FALLAS[1], [2 * cm, 3.5 * cm, 2.5 * cm, 1.5 * cm, 1.2 * cm, 5.5 * cm])''',
     r'''tabla(FALLAS[0], FALLAS[1], [2 * cm, 3.5 * cm, 2.5 * cm, 1.5 * cm, 1.5 * cm, 5.2 * cm])'''),

    ("18 Sección 8.3 actualizada: BCCh resuelto, demo de falla e idempotencia",
     bloque('      Paragraph("8.3 Fallas en APIs/Airflow y respuesta del diseño", H2), Paragraph(\n',
            '      Paragraph("9. Conclusiones", H1)'),
     r'''      Paragraph("8.3 Fallas en APIs/Airflow y respuesta del diseño", H2), Paragraph(
          f"Durante las pruebas el DAG del BCCh falló en validar_api (Codigo -5 «Invalid username or password») porque las "
          f"credenciales de la API aún no estaban activadas en el portal del Banco Central: se registraron los reintentos (RETRY) "
          f"y la falla final (FAILED) en audit.etl_audit, pero el DAG de Mercado Público siguió cargando y el DAG de KPIs se "
          f"disparó con su Dataset, demostrando el aislamiento de fallas. Tras activar las credenciales se ejecutó la recarga "
          f"histórica con «Trigger DAG w/ config» y quedaron {fmt(BCCH_N)} observaciones en dw.fact_indicador_economico, lo que "
          f"habilitó la conversión de montos a CLP y UF (KPI4). En la demostración de falla controlada la Variable mp_base_url "
          f"apuntó a una URL incorrecta: validar_api falló con HTTP 404, Airflow aplicó los reintentos con espera exponencial y "
          f"registró cada intento, los otros DAGs siguieron funcionando y, al restaurar la Variable y hacer Clear, el run se "
          f"recuperó con éxito. También se re-ejecutó un día ya cargado para verificar la idempotencia: los conteos de staging y "
          f"dw no cambiaron.{' Mercado Público respondió ocasionalmente con HTTP 429, que absorbieron los reintentos.' if N429 else ''}", P),
'''),

    ("19 Conclusiones según los resultados reales + Anexo A con las consultas SQL",
     bloque('          "licitado en términos reales. Los resultados muestran', 'doc = SimpleDocTemplate('),
     r'''          f"licitado en términos reales. En {MES} cumplen su meta: {', '.join(CUMPLEN) or 'ninguno'}; "
          f"no la cumplen: {', '.join(NO_CUMPLEN) or 'ninguno'}. Los indicadores que no cumplen son las "
          "principales oportunidades de mejora para las unidades de compra. Como trabajo futuro se propone "
          "incorporar órdenes de compra y proveedores, y completar el detalle de todas las licitaciones con "
          "más cuota de API.", P)]

s += [PageBreak(), Paragraph("Anexo A. Consultas SQL de evidencia", H1),
      Paragraph("Consultas usadas para verificar la carga, la idempotencia, la auditoría y el cruce de fuentes "
                "(sql/consultas/consultas_evidencia.sql):", P)]
ev = SQL_DIR / "consultas" / "consultas_evidencia.sql"
if ev.exists():
    s += [Preformatted(ev.read_text(encoding="utf-8")[:7000], CODE)]
s += [Paragraph("Cálculo de los indicadores (sql/kpi/kpis.sql, extracto):", P),
      Preformatted((SQL_DIR / "kpi" / "kpis.sql").read_text(encoding="utf-8")[:4500], CODE)]

'''),

    ("20 Lámina 1 con los integrantes reales",
     r'''"Integrantes: [Nombre 1] · [Nombre 2] · [Nombre 3]"])''',
     r'''"Integrantes: " + " · ".join(INTEGRANTES)])'''),

    ("21 Lámina 'Modelo de datos' con el diagrama",
     r'''"Esquemas: raw · staging · dw · mart · audit"])''',
     r'''"Esquemas: raw · staging · dw · mart · audit"], FIG / "modelo_dimensional.png")'''),

    ("22 Lámina 'Tolerancia a fallas' con la captura de la recuperación",
     r'''"4. Restaurar Variable + Clear → recuperación"], CAP / "03_grid_bcch.png")''',
     r'''"4. Restaurar Variable + Clear → recuperación"], CAP / "07_grid_recuperado.png")'''),
]

problemas = [f"  - {desc}: el fragmento original aparece {0 if viejo is None else fuente.count(viejo)} veces"
             for desc, viejo, _ in CAMBIOS if viejo is None or fuente.count(viejo) != 1]
if problemas:
    print("No se modificó el archivo. Fragmentos con problemas:")
    print("\n".join(problemas))
    print("¿Ya aplicaste las mejoras? Vuelve a copiar el original desde referencia/ y reintenta.")
    sys.exit(1)

nuevo = fuente
for desc, viejo, reemplazo in CAMBIOS:
    nuevo = nuevo.replace(viejo, reemplazo, 1)
    print("OK ", desc)
ruta.write_text(nuevo, encoding="utf-8")
print(f"\n{len(CAMBIOS)} mejoras aplicadas en {ruta}.")
print("Siguiente paso: edita la lista INTEGRANTES (cerca de la línea 30) con los nombres reales.")
