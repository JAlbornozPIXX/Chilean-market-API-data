# Issue 5 · Indicadores de gestión (KPIs), DAG de indicadores, informe PDF y presentación

> **Responsable:** Sofia (@Sofia-espinosa) · **Etiqueta:** `kpis-entregables` · **Hitos:** ETL e integración / Entrega
> **Ramas:** `feature/5-kpis` (**PR A**: KPIs y DAG, el **mar 27-10**) · `feature/5-informe-presentacion` (**PR B**: informe y PPTX, el **vie 30-10**)
> **Revisan tus PR:** Gustavo (@Rhezm) el PR A. En el PR B, **todos**: cada uno revisa su sección.
> **Depende de:** el PR A, de las Issues 1 y 2. El PR B, de la integración (Fase 3), del PR B de la Issue 2 y de las capturas de la Issue 1.
> **Entrega final:** **dom 01-11** (PDF + .zip + .pptx). Presentación el **jue 05-11**.

Documentos de apoyo: [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) · [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md) · [Manual de GitHub](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/02_MANUAL_GITHUB.md) · [Guía de presentación y defensa](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md)

---

## 1. Tu misión en una frase

Calcular los **6 indicadores de gestión en SQL** (esquema `mart`), con su DAG que se **dispara solo** cuando llegan datos nuevos. Después, producir los **entregables finales** (informe PDF y presentación PPTX) **con datos reales**, integrando los aportes de todo el equipo, para cumplir **cada** apartado de la pauta.

## 2. ¿Por qué es importante? (lo que pide la pauta)

| Criterio de la pauta | Qué depende de ti |
|---|---|
| **Indicadores de gestión** (5 pts): *"De 4 a 6 indicadores relevantes, con definición completa (objetivo, fórmula, fuente, periodicidad y meta), calculados correctamente en SQL sobre el modelo"* | `kpis.sql` y la tabla de definiciones del informe |
| **Diseño de DAGs** (5 pts): *"un DAG de indicadores disparado automáticamente"* | `dag_kpis.py` (Datasets) |
| **Estrategia de datos** (5 pts): *"Capítulo del informe de 2 a 3 páginas"* que aborde decisiones, calidad, origen, frecuencia, riesgos y escalabilidad *"con fundamento en el trabajo realizado"* | El capítulo 6. En la referencia medía **~1,3 páginas**; con las mejoras queda en 2 |
| **Informe** (5 pts): *"todos los apartados solicitados y evidencia completa"* | Todo el PDF |
| **Presentación** (30 % de la nota): *"8 a 12 láminas: problema y fuentes, arquitectura, diseño de DAGs, modelo de datos, indicadores y hallazgos, estrategia de datos y lecciones aprendidas"* | El PPTX (10 láminas) |

## 3. Conceptos que debes dominar

Están explicados en detalle en el [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md).

- **KPI (indicador clave de desempeño):** número que responde una pregunta de gestión y apoya una decisión. Se define con nombre, objetivo, fórmula, fuente, periodicidad y meta.
- **Meta, umbral y sentido:**
  - La *meta* es el valor esperado.
  - El *umbral* es el valor que gatilla una alerta.
  - El *sentido* dice si "mayor es mejor" o "menor es mejor".
- **Data mart (`mart`):** capa final con los indicadores listos para consumir.
- **Agregación (`GROUP BY`) y `count(*) FILTER (WHERE ...)`:** contar solo las filas que cumplen una condición dentro de cada grupo.
- **`NULLIF(x, 0)`:** convierte 0 en NULL para que una división por cero dé NULL en vez de un error.
- **CTE (`WITH ...`):** subconsulta con nombre que hace legible una consulta larga.
- **`UNION ALL`:** apila los resultados de varias consultas.
- **Funciones de ventana:** `row_number() OVER (PARTITION BY ... ORDER BY ...)` da el ranking dentro de cada mes, y `sum(...) OVER (PARTITION BY ...)` da el total del mes sin agrupar las filas.
- **Transacción con DELETE + INSERT:** recalcular todo el mart de forma atómica: quien lee ve la versión vieja o la nueva, nunca una mitad.
- **Vista (`VIEW`):** consulta guardada con nombre.
- **Data-aware scheduling, Dataset, `outlets` y la condición OR (`|`):** el DAG de KPIs no tiene horario. Corre cuando un DAG productor termina bien y "publica" que su tabla cambió. En Airflow 3 los Datasets se llaman *Assets*, pero nosotros usamos la versión 2.10.
- **Entorno virtual (`venv`) y `pip`:** instalación aislada de las librerías de Python del generador.
- **reportlab, python-pptx y matplotlib:** generan el PDF, el PPTX y los gráficos.

## 4. Archivos que te pertenecen

```
proyecto_etl/sql/kpi/kpis.sql                    (PR A)
proyecto_etl/dags/dag_kpis.py                    (PR A)
proyecto_etl/docs/generar_entregables.py         (PR B)
proyecto_etl/docs/figuras/*.png                  (PR B, salvo modelo_dimensional.png, que es de la Issue 2)
entregables/Informe_E2_INF522.pdf                (PR B)
entregables/Presentacion_E2_INF522.pptx          (PR B)
```

**Solo tú** generas y subes el PDF, el PPTX y las figuras. Si dos personas suben versiones distintas de un PDF, el conflicto **no se puede mezclar**. Los demás te envían sus textos y correcciones como **comentarios en esta issue**.

## 5. Paso a paso — PR A (KPIs y DAG de indicadores)

### Paso 1 — Rama y archivos

```bash
git switch main && git pull
git switch -c feature/5-kpis
git push -u origin feature/5-kpis
mkdir -p proyecto_etl/sql/kpi
cp referencia/proyecto_etl/sql/kpi/kpis.sql proyecto_etl/sql/kpi/
cp referencia/proyecto_etl/dags/dag_kpis.py proyecto_etl/dags/
```

### Paso 2 — Entiende cada indicador

La **pregunta de gestión** es: *¿Qué tan eficiente, competitivo y transparente es el proceso de licitaciones del Estado y cuánto se licita en términos reales?*

| KPI | Nombre | Fórmula (SQL sobre `dw`) | Meta | Sentido | Decisión que apoya | Usuario |
|---|---|---|---|---|---|---|
| KPI1 | Tasa de adjudicación (%) | 100 × adjudicadas (`es_exitoso`) / licitaciones con estado final (`es_final`: desierta, adjudicada, revocada) | ≥ 80 % | mayor | Detectar unidades con procesos fallidos, para capacitar o rediseñar las bases | Jefaturas de abastecimiento |
| KPI2 | Tasa de licitaciones desiertas (%) | 100 × desiertas (estado 7) / licitaciones con estado final | ≤ 15 % | menor | Falta de oferentes o bases mal formuladas | Jefaturas, ChileCompra |
| KPI3 | Plazo promedio publicación–cierre (días) | `AVG(dias_publicacion_cierre)`, solo con detalle y entre 0 y 365 días | ≥ 10 días | mayor | Dar tiempo suficiente para ofertar (competencia) | Jefaturas de abastecimiento |
| KPI4 | Monto estimado licitado (miles de UF) | `SUM(monto_estimado_uf) / 1000` (**cruce con el BCCh**) | Umbral de alerta: variación > ±20 % respecto del mes anterior | info (seguimiento) | Dimensionar el gasto comprometido sin efecto de la inflación ni del tipo de cambio | DIPRES, equipos de finanzas |
| KPI5 | Licitaciones con monto estimado visible (%) | 100 × con monto / licitaciones con detalle (`cantidad_items` no nulo) | ≥ 50 % | mayor | Promover la transparencia del monto | ChileCompra, Contraloría |
| KPI6 | Concentración top-10 organismos en monto (%) | Σ monto CLP de los 10 organismos con mayor monto / Σ monto CLP total | ≤ 60 % | menor | Detectar dependencia de pocos compradores | ChileCompra |

Todos tienen **fuente** Mercado Público, salvo KPI4 y KPI6, que usan Mercado Público + BCCh. La **periodicidad** es mensual, aunque se recalculan en cada actualización.

En el proyecto de referencia, con el mes 2026-10 incompleto, los resultados fueron:

- KPI1 = 81,06 (cumple)
- KPI2 = 18,94 (no cumple)
- KPI3 = 15,79 (cumple)
- **KPI4 = "—"**, porque no había datos del BCCh
- KPI5 = 55,45 (cumple)
- KPI6 = 80,85 (no cumple)

Con la carga final, **estos números cambiarán**, y el texto del informe debe reflejar los valores nuevos.

> **¿Por qué KPI4 ahora tiene un umbral?** La pauta exige *"meta o umbral"* para **cada** indicador, y en la referencia KPI4 decía "sin meta". Como es un indicador de magnitud, no tiene sentido ponerle una meta fija: definimos un **umbral de alerta**. Si el monto real varía más de ±20 % respecto del mes anterior, hay que revisar qué pasó, por ejemplo una licitación gigante puntual o una caída de actividad. Ese umbral **se puede evaluar desde el segundo mes completo**, y así se dice en la defensa.

### Paso 3 — Entiende `kpis.sql` técnicamente

Los números de línea son los del archivo de `referencia/`.

| Líneas | Qué hace | Detalle |
|---|---|---|
| 7–8 | `DELETE` de `mart.kpi_mensual` y `mart.top_organismos_mes` | Recalcula **todo**. Como corre dentro de **una transacción** (`pg_cursor`), nadie ve el mart vacío |
| 10–15 | CTE `lic`: une `fact_licitacion` con `dim_tiempo` (para el `anio_mes`) y `dim_estado_licitacion` (para `es_final` y `es_exitoso`) | Las banderas del estado están en la **dimensión**, no repetidas en el hecho |
| 16–44 | CTE `k`: 5 `SELECT` (KPI1 a KPI5) unidos con `UNION ALL`, cada uno con `GROUP BY anio_mes` | `100.0 *` fuerza la división decimal: con enteros, 1/3 daría 0. `NULLIF(...,0)` evita dividir por cero |
| 45–49 | `INSERT` en `kpi_mensual`, redondeado a 4 decimales | `cumple_meta` con `CASE`: si el sentido es *mayor*, `val >= meta`; si es *menor*, `val <= meta`; si no hay meta o valor, NULL ("N/A") |
| 51–65 | KPI6, parte 1: suma el monto CLP por mes y organismo; `row_number()` arma el ranking y `sum() OVER (PARTITION BY anio_mes)` calcula la participación %; guarda el top 10 | Las funciones de ventana calculan el ranking y el total **sin perder el detalle por organismo** |
| 67–70 | KPI6, parte 2: valor = suma de la participación del top 10; meta 60; cumple si es ≤ 60 | — |
| 73–79 | Vista `mart.v_contexto_economico_mes`: promedio mensual y volatilidad (desviación estándar de la variación diaria) por serie del BCCh | Da contexto a KPI4 (por ejemplo, cuánto se movió el dólar ese mes) |

### Paso 4 — Entiende `dag_kpis.py`

| Elemento | Qué hace | Por qué |
|---|---|---|
| `DS_BCCH` y `DS_MP` (líneas 20–21) | Los mismos URI que declaran los DAG productores en `outlets` | Airflow los compara **como texto**: una letra distinta y el DAG nunca se dispara |
| `schedule=(DS_BCCH \| DS_MP)` (52) | Corre cuando **cualquiera** de las dos tablas de hechos se actualiza | Con **AND** (`&`), si el BCCh fallara, los KPIs **nunca** se recalcularían: quedarían acoplados. Con OR se **aíslan las fallas**. La condición OR existe en Airflow 2.10 |
| `catchup=False`, `max_active_runs=1` | No reprocesa el pasado ni corre en paralelo | Cada ejecución recalcula todo el mart, así que basta con una |
| `retries=2`, `retry_delay=1 min`, `execution_timeout=10 min`, callbacks | Tolerancia a fallas, igual que en los ETL | Las fallas también quedan en `audit.etl_audit` |
| `verificar_frescura()` (25–33) | Revisa la última carga de cada fuente y **advierte** si tiene más de 3 días | **No bloquea**: los KPIs se calculan con lo último válido y queda la advertencia en el log |
| `calcular_kpis()` (36–45) | Ejecuta `kpis.sql` completo en **una** transacción y audita las filas y el % de nulos | Un error deshace todo y el mart queda como estaba |

Solo se emite el Dataset si la tarea con `outlets` (`control_calidad`) **termina bien**. Si falla, no se emite y los KPIs no se recalculan con datos malos.

### Paso 5 — Prueba

```bash
git fetch origin && git merge origin/main
cd proyecto_etl && docker compose up -d
docker compose exec airflow-scheduler airflow dags list-import-errors           # "No data found"
docker compose exec airflow-scheduler airflow dags unpause kpis_gestion_compras_publicas
docker compose exec airflow-scheduler airflow dags trigger kpis_gestion_compras_publicas
```

`kpis.sql` funciona incluso con las tablas vacías, porque ya se probó en PostgreSQL 16. En ese caso, el mart simplemente queda vacío.

Cuando haya datos (después de los merges de las Issues 3 y 4, o con tus propios tickets):

```bash
docker compose exec postgres psql -U etl -d dw_gestion \
  -c "SELECT anio_mes, kpi_codigo, round(valor,2) AS valor, meta, cumple_meta FROM mart.kpi_mensual ORDER BY 1,2" \
  -c "SELECT * FROM mart.top_organismos_mes ORDER BY anio_mes, ranking LIMIT 10"
```

Comprueba también que funciona el disparo automático:

1. Abre el menú **Datasets** de la UI: debes ver los 2 ETL apuntando al DAG de KPIs.
2. Lanza **solo** el DAG del BCCh y verifica que, al terminar, el DAG de KPIs corre solo. Eso demuestra la condición OR.

### Paso 6 — Commit y PR A

```bash
cd ..
git add proyecto_etl/sql/kpi/kpis.sql proyecto_etl/dags/dag_kpis.py
git commit -m "feat(kpis): 6 indicadores en mart y DAG disparado por Datasets (#5)"
git push
```

Abre el PR con `Parte de #5`, para que la issue no se cierre todavía, y pide revisión a Gustavo.

## 6. Paso a paso — PR B (informe y presentación)

### Paso 7 — Entorno de Python para el generador, en la máquina de demo

El informe se genera **donde están los datos finales**: la laptop de Ivan. Hazlo con él.

```bash
cd <ruta>/Chilean-market-API-data
python3 -m venv .venv_docs                  # Windows: py -m venv .venv_docs
source .venv_docs/bin/activate              # Windows: .venv_docs\Scripts\activate
pip install python-pptx reportlab matplotlib psycopg2-binary
```

`.venv_docs/` está en `.gitignore`.

### Paso 8 — Copia el generador y aplica las 22 mejoras (ya probadas)

```bash
git switch main && git pull
git switch -c feature/5-informe-presentacion
git push -u origin feature/5-informe-presentacion
mkdir -p proyecto_etl/docs
cp referencia/proyecto_etl/docs/generar_entregables.py proyecto_etl/docs/
python3 planificacion/recursos/aplicar_mejoras_informe.py proyecto_etl/docs/generar_entregables.py
git diff --no-index referencia/proyecto_etl/docs/generar_entregables.py proyecto_etl/docs/generar_entregables.py
```

El script es **"todo o nada"**: si algo no calza, no cambia nada y te explica qué pasó. El último comando muestra cada cambio para que lo revises. Se probó de punta a punta: aplica los cambios, compila y genera el PDF y el PPTX sin errores.

Luego **edita la lista `INTEGRANTES`**, cerca de la línea 30, con los 5 nombres reales.

| # | Cambio | Por qué (problema del informe de referencia) |
|---|---|---|
| 01 | Constantes `INTEGRANTES` y `SQL_DIR` | Los nombres aparecían como "[Nombre 1] / [Nombre 2] / [Nombre 3]" |
| 02 | Conexión a `airflow_meta` | Para medir la duración real de cada DAG run |
| 03 | Tiempos con 2 decimales + tabla `dag_run` | El DAG de KPIs aparecía con "0,00" segundos por el redondeo, y faltaba el DAG del BCCh |
| 04 | Alias `organismo` en el Top 10 | La columna se llamaba "**left**" en el PDF |
| 05 | Métricas de calidad medidas, KPIs que cumplen o no, y conteo de HTTP 429 | El texto se arma con números reales y no con afirmaciones fijas |
| 06 | Nueva figura de arquitectura | El texto se salía de las cajas y la caja "mart" se encimaba con la del DAG de KPIs |
| 07 | Portada con los integrantes | Nombres reales |
| 08 | Fila "Límites" de Mercado Público | Datos exactos de los términos de ChileCompra: 10.000 por día, un ticket por persona, horario 22:00–07:00 |
| 09 | Diagrama del modelo en la sección 3.3 | No había diagrama del modelo dimensional. Usa el PNG de la Issue 2 |
| 10–11 | Leyendas de las capturas 03 y 07 | Decían "falla aislada" (el BCCh ya funciona) y tenían una fecha fija |
| 12 | Umbral de KPI4 | La pauta exige *meta o umbral* para cada KPI |
| 13 | Tabla de reglas de calidad con resultados **medidos** (6.2) | Estrategia *"con fundamento en el trabajo realizado"*, y suma extensión al capítulo |
| 14 | Matriz de riesgos (6.5): probabilidad, impacto, mitigación y evidencia | Antes eran solo viñetas |
| 15 | Nueva sección 6.7 Gobierno, seguridad y operación | Accesos, retención, monitoreo y documentación. Con 13, 14 y 15 el capítulo llega a **2 páginas** (antes ~1,3) |
| 16 | Dos tablas de tiempo en la sección 7 | Wall-clock (Airflow) y procesamiento (auditoría) |
| 17 | Ancho de la columna "intento" | El encabezado se cortaba en dos líneas |
| 18 | Sección 8.3 actualizada | Cuenta la historia completa: la falla `-5` del BCCh, el aislamiento, la activación y la recarga, más la demo y la idempotencia. La frase de HTTP 429 aparece **solo si la auditoría los registró** |
| 19 | Conclusiones con los KPIs reales + **Anexo A** con las consultas SQL | Antes las conclusiones eran un texto fijo. El anexo aporta la evidencia de *consultas* que menciona la pauta |
| 20–22 | Láminas: integrantes, diagrama del modelo y captura de la recuperación | La lámina de tolerancia a fallas mostraba la grilla del BCCh fallando |

### Paso 9 — Recibe los aportes del equipo y revisa los textos

Cada integrante te deja un **comentario en esta issue**. Tú los incorporas en `generar_entregables.py`, en los textos de las secciones indicadas:

| Quién | Qué te envía | Dónde va |
|---|---|---|
| Ivan | Correcciones de arquitectura y configuración | Secciones 3 y 4 |
| Gustavo | Resultado de la prueba de idempotencia (antes / después) y la revisión de la sección 7 | Secciones 3.3, 7 y 8.3 |
| Jairo | Números de la carga final de Mercado Público, el párrafo de la muestra aleatoria y hallazgos nuevos | Secciones 3.1, 6.2 y 8.1 |
| Benja | Rangos y conteos por serie, % convertidas a UF, historia de la falla `-5` y fecha de publicación más antigua | Secciones 2, 6.3 y 8.3 |

Revisa también estos textos fijos:

- El bullet de la sección 8.1 *"algunas creadas en 2024"*: confírmalo con el dato de Benja.
- La sección 3.1: agrega una frase sobre la **muestra aleatoria reproducible** del detalle (Issue 3).
- La sección 8.2: mantenla si siguen siendo verdaderos los problemas que lista.

### Paso 10 — Genera los entregables (en la máquina de demo)

```bash
cd proyecto_etl
PGPASSWORD='<POSTGRES_PASSWORD del .env>' python docs/generar_entregables.py
# Windows PowerShell:  $env:PGPASSWORD="<clave>"; python docs\generar_entregables.py
```

Los archivos salen en `entregables/`: `Informe_E2_INF522.pdf` y `Presentacion_E2_INF522.pptx`.

### Paso 11 — Revisa el PDF contra la pauta (página por página)

- [ ] **Portada** con los 5 nombres reales; **introducción** con la pregunta de gestión
- [ ] **Fuentes y justificación**: tabla de las 2 APIs (viabilidad, documentación, límites, volumen, licencia) y **campo de cruce**
- [ ] **Paso a paso**: extracción, transformación (las 9 transformaciones), carga y **diagrama del modelo**
- [ ] **DAGs**: tabla de schedules justificados, diagrama de dependencias, las **7 capturas** (exitosas **y** fallidas) y el **log** de la falla
- [ ] **Indicadores**: los 6 con definición completa (objetivo, fórmula, fuente, periodicidad y meta o umbral), resultados del mes y gráficos. **KPI4 con valor, no "—"**
- [ ] **Estrategia de datos**: **2 páginas o más** (6.1 a 6.7)
- [ ] **Información de carga**: registros por tabla, % de nulos o errores y **tiempo de cada uno de los 3 DAGs**
- [ ] **Análisis del proceso**: hallazgos, problemas con su solución y fallas con la respuesta del diseño
- [ ] **Conclusiones** coherentes con los números
- [ ] **Anexo A** con las consultas SQL
- [ ] Sin textos pendientes: busca en el PDF "Insertar", "Nombre Apellido" y "[Nombre"
- [ ] PPTX: 10 láminas (la pauta pide de 8 a 12), con nombres, imágenes y KPIs con valores

### Paso 12 — Commit, PR B y revisión de todos

```bash
cd ..
git add proyecto_etl/docs/generar_entregables.py proyecto_etl/docs/figuras/ entregables/Informe_E2_INF522.pdf entregables/Presentacion_E2_INF522.pptx
git status          # revisa que NO subas modelo_dimensional.png modificado ni nada fuera de tu lista
git commit -m "docs(entrega): informe y presentación generados con datos finales (#5)"
git push
```

Abre el PR con `Closes #5` y asigna como revisores a **todos**. Cada integrante revisa su sección y aprueba. Hazle merge el **vie 30-10**.

> Si regeneras después del merge (sáb 31-10), hazlo en una rama nueva `feature/5-regeneracion-final`, con su PR. Si quieren pulir el diseño del PPTX a mano en PowerPoint, háganlo **después** de la última regeneración: regenerar **sobrescribe** el archivo.

## 7. Criterios de aceptación (Definition of Done)

- [ ] `kpis_gestion_compras_publicas` aparece sin errores y se dispara solo cuando termina cualquiera de los ETL (condición OR)
- [ ] `mart.kpi_mensual` tiene los 6 KPIs del mes, y **KPI4 tiene valor**
- [ ] Puedo explicar la fórmula, la meta, el sentido y la decisión de cada KPI, y las líneas de `kpis.sql`
- [ ] El generador tiene las 22 mejoras y los nombres reales
- [ ] El PDF cumple el checklist del Paso 11, y la Estrategia de datos mide 2 páginas o más
- [ ] El PPTX tiene 10 láminas, con nombres, diagramas, capturas y KPIs correctos
- [ ] Todos los integrantes aprobaron el PR B

## 8. Tu parte en la presentación

- **Lámina 1 "Portada"** (~10 s): presentas al equipo y la pregunta de gestión.
- **Lámina 6 "Indicadores de gestión"** (~55 s): los 6 KPIs contra su meta y qué decisión apoyan.
- **Lámina 9 "Estrategia de datos"** (~40 s).
- Eres **dueño del PowerPoint**: llévalo en un pendrive y en la nube, y verifica que se vea bien en el computador de la sala.

## 9. Preguntas probables en la defensa (con respuesta guía)

1. **¿Qué pregunta de gestión responden sus indicadores?** *¿Qué tan eficiente, competitivo y transparente es el proceso de licitaciones del Estado y cuánto se licita en términos reales?*
   - **Eficiencia:** KPI1 y KPI2.
   - **Competencia:** KPI3 y KPI6.
   - **Transparencia:** KPI5.
   - **Magnitud real:** KPI4.
2. **¿Por qué KPI1 divide por las licitaciones con estado final y no por todas?** Porque una licitación publicada o cerrada todavía no termina: no se sabe si se adjudicará. Medir sobre las finalizadas (desierta, adjudicada, revocada) evita castigar procesos en curso.
3. **¿De dónde salen las metas (80 %, 15 %, 10 días, 50 %, 60 %)?** Son metas de referencia que definió el equipo para monitorear. Por ejemplo, que la mayoría de los procesos termine adjudicada, que los desiertos sean pocos y que el plazo dé tiempo real para ofertar. En una institución real se calibrarían con la normativa de compras y con la historia de varios meses.
4. **¿Por qué KPI4 no tiene una meta fija?** Porque es un indicador de magnitud: más monto no es ni bueno ni malo. Por eso tiene un **umbral de alerta** de ±20 % de variación respecto del mes anterior, que se puede evaluar desde el segundo mes completo.
5. **¿Cómo se calcula `cumple_meta`?** Con la columna `sentido`. Si es *mayor*, cumple cuando valor ≥ meta; si es *menor*, cuando valor ≤ meta; sin meta queda N/A.
6. **¿Por qué calculan los KPIs en SQL?** Lo pide la pauta, y además el SQL es declarativo, corre dentro de la base sin mover datos y cualquiera puede auditar la fórmula en `kpis.sql`.
7. **¿En qué se diferencian el `dw` y el `mart`?** El `dw` tiene el modelo dimensional con el detalle: una licitación por fila. El `mart` tiene los resultados agregados por mes, listos para consumir.
8. **¿Por qué el mart se recalcula con DELETE + INSERT?** Es pequeño, y recalcularlo completo garantiza consistencia aunque cambien datos antiguos. Al hacerlo en **una** transacción es atómico.
9. **¿Qué ve alguien que consulta el mart justo mientras se recalcula?** La versión anterior completa. PostgreSQL no muestra los cambios de una transacción hasta el `COMMIT`.
10. **¿Cómo se dispara el DAG de KPIs?**
    - Con *data-aware scheduling*: la tarea `control_calidad` de cada ETL declara `outlets=[Dataset(...)]`.
    - Cuando termina **bien**, Airflow marca el Dataset como actualizado.
    - El DAG de KPIs, con `schedule=(DS_BCCH | DS_MP)`, corre cuando cualquiera de los dos se actualiza.
11. **¿Por qué OR y no AND?** Con AND, una fuente caída bloquearía los indicadores. Con OR, los KPIs se recalculan con la fuente que sí llegó y con el último dato válido de la otra. Es el aislamiento de fallas que pide la pauta.
12. **¿Qué hace `verificar_frescura`?** Mira la última carga de cada fuente y deja una advertencia si tiene más de 3 días. No detiene el cálculo.
13. **¿Qué limitaciones tienen sus KPIs?**
    - KPI3 a KPI6 dependen del **detalle**, que está limitado por la cuota del ticket. Se calculan sobre una **muestra aleatoria**, así que son estimaciones.
    - El mes en curso es parcial.
    - KPI4 y KPI6 dependen de que el monto sea visible, y por eso existe KPI5.
14. **¿Cómo agregarían un KPI nuevo, por ejemplo uno por región?** Con un `SELECT` más en `kpis.sql`, agrupando por `dim_organismo.region`. El modelo ya lo soporta: es escalabilidad "en más indicadores".
15. **¿Cómo se genera el informe?** Con un script de Python que **consulta la base real** (auditoría, dw y mart), arma tablas y gráficos con matplotlib y produce el PDF con reportlab y el PPTX con python-pptx. Así los números del informe siempre coinciden con los datos.
16. **¿Por qué el informe trae dos tiempos por DAG?**
    - La duración de Airflow (`dag_run`) es el tiempo de reloj del run completo, incluida la espera entre tareas.
    - La de la auditoría es la suma del procesamiento de cada tarea.
    - Juntas muestran cuánto pesa la orquestación.

## 10. Si te atascas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `KeyError: 'PGPASSWORD'` | No definiste la variable | `PGPASSWORD='...' python docs/generar_entregables.py` |
| `psycopg2.OperationalError: connection refused` | El stack no está levantado o el puerto es otro | Ejecuta `docker compose ps`. El puerto es **5433** |
| `ModuleNotFoundError: No module named 'pptx'` | El venv no está activo o falta un paquete | Activa el venv; el paquete se llama `python-pptx` y el módulo `pptx` |
| El PDF muestra `[Insertar captura: ...]` | Falta un PNG en `docs/capturas/` o en `docs/figuras/` | Pídelo a su dueño: capturas → Issue 1, diagrama → Issue 2 |
| El script de mejoras dice "aparece 0 veces" | Ya lo aplicaste, o el archivo no es el original | Vuelve a copiar desde `referencia/` y repite |
| El DAG de KPIs no se dispara solo | El URI del Dataset no coincide, o el ETL terminó en error (sin outlet) | Compara los URI letra por letra y revisa que `control_calidad` haya quedado verde |
| KPI4 vacío | No hay datos del BCCh o MP cargó antes | Issue 4: recarga histórica y *Clear* de `cargar_dw` de MP con *Downstream* |
| Un texto con `<b>` rompe el PDF | Etiqueta sin cerrar en un `Paragraph` | Toda etiqueta `<b>` o `<i>` debe cerrarse (`</b>`) |
| La 1ª ejecución tarda | matplotlib arma su caché de fuentes | Es normal y solo pasa la primera vez |

Si después de 30 minutos sigues bloqueado, comenta en esta issue con el error exacto y etiqueta `bloqueante`.
