# Issue 4 · ETL Banco Central de Chile (dólar, euro, UF, UTM): credenciales, recarga histórica y cruce de monedas

> **Responsable:** Benja (@Sonnyxpj) · **Etiqueta:** `etl-bcch` · **Hito:** ETL e integración
> **Rama:** `feature/4-etl-bcch` · **Revisa tu PR:** Jairo (@JAlbornozPIXX)
> **Depende de:** Issues 1 y 2 (PR A) en `main`.
> **Fechas:** credenciales activas **hoy, vie 23-10** · PR listo el **lun 26-10** · recarga histórica en la máquina de demo el **mié 28-10**

Documentos de apoyo: [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) · [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md) · [Manual de GitHub](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/02_MANUAL_GITHUB.md) · [Guía de presentación y defensa](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md)

---

## 1. Tu misión en una frase

Llevar al repositorio el **ETL del Banco Central** y su DAG. Este ETL trae el dólar observado, el euro, la UF y la UTM.

Además debes:

- **activar las credenciales de la API**: el proyecto de referencia falló justo aquí, con 0 datos del BCCh y el KPI4 vacío;
- hacer la **recarga histórica desde 2024**;
- garantizar que el **cruce** funcione, es decir, que los montos de las licitaciones se conviertan a pesos y a UF.

## 2. ¿Por qué es importante? (lo que pide la pauta)

**Esta es la brecha más grande del proyecto de referencia.** El informe entregado mostraba lo siguiente:

- `raw.bcch_series`, `staging.bcch_observacion` y `dw.fact_indicador_economico` con **0 filas**.
- Todas las ejecuciones del DAG en estado RETRY o FAILED, con `Codigo=-5 Invalid username or password`.
- El **KPI4** (monto en UF) vacío ("—").
- El cruce entre fuentes solo explicado, nunca demostrado con datos.

| Criterio de la pauta | Qué depende de ti |
|---|---|
| **Selección y justificación de fuentes** (5 pts): *"con **campo de cruce claro**"* | Que el cruce por fecha y moneda funcione **con datos reales** |
| **Extracción y transformación** (5 pts): *"autenticación ... errores HTTP ... validaciones de calidad"* | `bcch.py`: autenticación, `Codigo≠0`, NaN, duplicados, atípicos y rangos |
| **Diseño de DAGs** (5 pts): *"schedule ... justificado"* y *"recargar un rango de fechas pasado"* | `dag_etl_bcch.py` y la recarga histórica |
| **Indicadores** (5 pts): *"calculados correctamente"* | KPI4 y KPI6 necesitan los montos convertidos |
| Instrucción 1: *"Si la API requiere clave, el grupo debe tramitarla con anticipación"* | Activar hoy las credenciales |

## 3. Conceptos que debes dominar

Están explicados en detalle en el [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md).

- **Serie de tiempo:** secuencia de valores fechados de un mismo indicador, por ejemplo el dólar de cada día. Su **frecuencia** puede ser diaria (D) o mensual (M).
- **Dólar observado:** tipo de cambio oficial pesos/dólar que publica el Banco Central para los días hábiles.
- **UF (Unidad de Fomento):** unidad de cuenta que se **reajusta según la inflación** (IPC) y tiene un valor para **cada día**. Expresar montos en UF elimina el efecto de la inflación; eso significa "**términos reales**".
- **UTM (Unidad Tributaria Mensual):** unidad que cambia una vez al mes. En compras públicas define los tramos de las licitaciones: según la documentación de Mercado Público, **L1** es menor a 100 UTM, **LE** va de 100 a 1.000 UTM y **LP** es mayor a 1.000 UTM.
- **GetSeries:** la función de la API del BCCh que entrega una serie entre dos fechas.
- **Error de negocio (`Codigo≠0`):** la API del BCCh responde **HTTP 200 aunque haya un error**. El error real viene en el campo `Codigo`. Por ejemplo, `-5` significa credenciales inválidas.
- **NaN:** "no es un número". Aparece en días sin publicación, como los feriados. Lo tratamos como nulo.
- **Ventana móvil de 7 días:** cada run vuelve a pedir los últimos 7 días, para recoger publicaciones tardías o correcciones. No duplica nada, gracias al upsert.
- **Función de ventana `lag()`:** obtiene el valor de la **fila anterior** dentro de un grupo. Con ella calculamos la variación diaria.
- **Valor atípico (*outlier*):** un dato anormal. Aquí, una variación diaria mayor al 15 %, que probablemente es un error.
- **catchup, params y Trigger DAG w/ config:** formas de cargar períodos pasados.
- **Precedencia de Variables:** una Variable definida por **variable de entorno** (`AIRFLOW_VAR_BCCH_PASS`) gana sobre una creada en la UI o la CLI.

## 4. Archivos que te pertenecen

```
proyecto_etl/dags/etl_lib/bcch.py
proyecto_etl/dags/dag_etl_bcch.py
```

**No toques:**

- `common.py` (Issue 1) ni `01_modelo.sql` (Issue 2). Las series y monedas de `dim_moneda` están en el DDL: si necesitas una moneda nueva, pídela en la Issue 2.
- `mercadopublico.py` (Issue 3).

## 5. Paso a paso

### Paso 0 — Registra y **activa** tus credenciales (Día 0: HOY)

1. Entra a **https://si3.bcentral.cl/Siete/es/Siete/API** y elige **"Registrarse"**. Usa tu correo y una clave **sin** `$`, `#`, comillas, `\` ni espacios, para que no haya problemas en `.env`.
2. Si te llega un correo de confirmación, ábrelo y confirma.
3. Vuelve a la misma página y entra como **"Usuario Registrado"**. La página dice textualmente *"Para activar las credenciales debe iniciar sesión"*: inicia sesión y **activa las credenciales de la API**.
4. Prueba en el navegador. **No compartas esta URL: lleva tu clave.**
   ```
   https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user=TU_CORREO&pass=TU_CLAVE&function=GetSeries&timeseries=F073.UFF.PRE.Z.D&firstdate=2026-10-01&lastdate=2026-10-10
   ```
   - **Bien:** `"Codigo":0` y una lista `Series` → `Obs`, cuyos elementos traen `indexDateString` (`01-10-2026`), `value` y `statusCode`.
   - **Mal:** `Codigo -5` con *"Invalid username or password"*. Significa que las credenciales no están activadas o están mal escritas. Es **exactamente el error del proyecto de referencia**.
5. Avisa en esta issue cuando tengas `Codigo 0`, pero **sin pegar tu clave**.

### Paso 1 — Crea tu rama y copia los archivos

```bash
git switch main && git pull
git switch -c feature/4-etl-bcch
git push -u origin feature/4-etl-bcch
cp referencia/proyecto_etl/dags/etl_lib/bcch.py proyecto_etl/dags/etl_lib/
cp referencia/proyecto_etl/dags/dag_etl_bcch.py proyecto_etl/dags/
```

### Paso 2 — Entiende las 4 series

| `serie_id` | Nombre | Unidad | Convierte la moneda | Frecuencia | Para qué la usamos |
|---|---|---|---|---|---|
| `F073.TCO.PRE.Z.D` | Dólar observado | CLP/USD | `USD` | Diaria (días hábiles) | Pasar a pesos las licitaciones en dólares |
| `F072.CLP.EUR.N.O.D` | Euro (tipo de cambio) | CLP/EUR | `EUR` | Diaria | Pasar a pesos las licitaciones en euros |
| `F073.UFF.PRE.Z.D` | Unidad de Fomento | CLP/UF | `CLF` | Diaria (todos los días) | Expresar **todos** los montos en UF (KPI4) y convertir las licitaciones en UF |
| `F073.UTR.PRE.Z.M` | Unidad Tributaria Mensual | CLP/UTM | `UTM` | Mensual | Pasar a pesos las licitaciones en UTM |

La columna "Convierte la moneda" coincide con `dw.dim_moneda.serie_id_bcch` del DDL. Ese es el **puente del cruce**: cada licitación trae su `Moneda`; `dim_moneda` dice qué serie del BCCh la convierte; y `dim_tiempo` empareja la fecha.

### Paso 3 — Entiende `bcch.py`, función por función

Los números de línea son los del archivo de `referencia/`.

| Función (líneas) | Qué hace | Lo que debes poder explicar |
|---|---|---|
| `SERIES` y `UMBRAL_ATIPICO_PCT` (22–29) | El diccionario de las 4 series (nombre, unidad, moneda, frecuencia) y el umbral de atípico (15 %) | Agregar una serie nueva es agregar una línea aquí. Es **escalabilidad** |
| `_cfg()` (32–34) | Lee las Variables `bcch_base_url` (con valor por defecto), `bcch_user` y `bcch_pass` | La clave nunca está en el código: viene de `.env` |
| `_consultar()` (37–44) | Arma los parámetros (`user`, `pass`, `function=GetSeries`, `timeseries`, `firstdate`, `lastdate`) y llama con **1 s de pausa**. Si `Codigo != 0`, lanza `ApiError` con el código y la descripción | La API responde **HTTP 200 incluso con error**: si no se revisara `Codigo`, se cargaría "nada" sin que nadie lo note |
| `validar_api()` (47–53) | Pide la UF del mes actual con un solo reintento HTTP | Es la **validación previa**: comprueba de una vez que la API responde **y** que las credenciales son válidas |
| `rango_fechas()` (56–64) | Si hay `params`, usa ese rango. Si no, usa el `data_interval` del run **más 7 días hacia atrás** | Es la ventana móvil: recoge correcciones y publicaciones tardías, y el upsert evita duplicar |
| `extraer()` (67–83) | **Una llamada por serie** para todo el rango, guardada tal cual en `raw.bcch_series` con `http_status`, `run_id` y fecha y hora | Esta API entrega todo el rango en una respuesta, así que aquí "paginar" es ir **por serie** |
| `_a_numero()` (86–95) | `'945,12'` pasa a `945.12`. `'NaN'`, `'NeuN'`, valores no numéricos o ≤ 0 pasan a `None` | Conversión de tipos y tratamiento de nulos |
| `transformar()` (98–124) | Toma el raw **de este run** (`DISTINCT ON` la última respuesta por serie) y convierte `indexDateString` (`%d-%m-%Y`) a fecha. **Deduplica** por fecha con un `set`. Si el `statusCode` no es `OK`, el valor queda nulo. Después hace upsert en `staging.bcch_observacion` | `es_nulo` marca feriados o días sin dato. Las filas nulas se cuentan como **rechazadas** en la auditoría |
| `cargar_dw()` (127–162) | Asegura `dim_tiempo` (años completos) y hace upsert de `dim_serie`. Calcula la **variación %** con `lag()` sobre **toda** la serie en staging, filtra el rango, descarta nulos y atípicos (variación > 15 %, **excepto** la serie mensual) y hace upsert en `fact_indicador_economico` | Como el `lag()` se calcula antes de filtrar por fecha, el primer día del rango igual tiene su variación respecto del día anterior. La UTM es mensual: su cambio entre meses no se compara con un umbral diario |
| `control_calidad()` (165–177) | Falla si alguna **UF** está fuera de 20.000–80.000 o algún **dólar** fuera de 300–2.000 | Si falla, **no se emite el Dataset** y los KPIs no se recalculan con valores absurdos |

### Paso 4 — Entiende el DAG `dag_etl_bcch.py`

| Parámetro | Valor | Justificación (dila así en la defensa) |
|---|---|---|
| `dag_id` | `etl_bcch_indicadores` | Un DAG **independiente** por API |
| `schedule` | `"0 10 * * 1-5"` | De **lunes a viernes a las 10:00**. Las series diarias del BCCh se publican en días hábiles, y la ventana de 7 días cubre fines de semana, feriados y publicaciones tardías |
| `catchup` | `False` | La API entrega **cualquier rango en una sola llamada**: el histórico se carga con **un** run manual con `params`, que es más eficiente que cientos de runs de catchup. El DAG de Mercado Público sí usa catchup, porque su API entrega **un día por llamada** |
| `max_active_runs` | `1` | No se superponen runs que escriben las mismas filas |
| `retries=3`, `retry_delay=1 min`, `retry_exponential_backoff=True`, `max_retry_delay=10 min` | — | Las fallas temporales se recuperan solas |
| `execution_timeout=15 min` | — | Son pocas llamadas: si tarda más, algo anda mal |
| `params` `fecha_desde` / `fecha_hasta` | `Param(None, type=["null","string"], format="date")` | Permiten la **recarga de un rango pasado** |
| `validar_api` con `retries=2` y timeout de 2 min | — | Detecta rápido una API caída o credenciales inválidas |
| `control_calidad` con `outlets=[DS_BCCH]` | `Dataset(".../dw/fact_indicador_economico")` | Si termina bien, **dispara el DAG de KPIs** |

### Paso 5 — Aplica las 3 mejoras

**Mejora 1 — Enlace de documentación correcto.**

- **Problema:** el docstring de `bcch.py` (línea 3) apunta a `https://si3.bcentral.cl/estadisticas/Principal1/Web_Services/index_EN.htm`. Al abrirla, hoy responde **"Página no encontrada"**.
- **Solución:** cambia las líneas 3 a 5 por:

```python
Portal de la API (registro, activación de credenciales y documentación): https://si3.bcentral.cl/Siete/es/Siete/API
Requiere usuario y clave: registro gratuito y ACTIVACIÓN de las credenciales iniciando sesión en el
portal. Se guardan como Airflow Variables `bcch_user` y `bcch_pass` (inyectadas desde .env).
```

**Mejora 2 — Ejemplo de recarga histórica en el docstring del DAG.** Cambia la línea 6 de `dag_etl_bcch.py` por:

```python
Recarga histórica: Trigger DAG w/ config {"fecha_desde":"2024-01-01","fecha_hasta":"<hoy>"}
(desde 2024 porque hay licitaciones con movimiento en 2026 que se publicaron en 2024)
```

**Mejora 3 — Justifica la fecha de inicio con datos.** La conversión usa el valor vigente a la **fecha de publicación**, así que el BCCh debe cubrir desde la licitación publicada más antigua. Cuando tengas datos de Mercado Público en tu base local (Issue 3), ejecuta:

```sql
SELECT min(fecha_publicacion) AS publicacion_mas_antigua FROM staging.mp_licitacion;
```

Usa como `fecha_desde` el 1 de enero de ese año (en la referencia había licitaciones de 2024). Anota el resultado en esta issue: es un argumento para la defensa.

### Paso 6 — Prueba en tu computador

```bash
git fetch origin && git merge origin/main
cd proyecto_etl
# .env con TU BCCH_USER (correo) y BCCH_PASS activados
docker compose up -d

docker compose exec airflow-scheduler airflow dags list-import-errors        # esperado: "No data found"
docker compose exec airflow-scheduler airflow tasks test etl_bcch_indicadores validar_api 2026-10-20
#   esperado: "API BCCh disponible y credenciales válidas". Si ves Codigo=-5, vuelve al Paso 0.

docker compose exec airflow-scheduler airflow dags unpause etl_bcch_indicadores
#   al activarlo corre 1 run (el último intervalo de lunes a viernes) con su ventana de 7 días
```

**Recarga histórica.** Elige una de estas dos formas:

- **UI:** ▶ → **Trigger DAG w/ config** → `{"fecha_desde": "2024-01-01", "fecha_hasta": "2026-10-28"}`. Usa la fecha de hoy en `fecha_hasta`.
- **CLI** (en macOS, Linux o WSL; en PowerShell las comillas fallan, así que usa la UI):
  ```bash
  docker compose exec airflow-scheduler airflow dags trigger etl_bcch_indicadores -c '{"fecha_desde": "2024-01-01", "fecha_hasta": "2026-10-28"}'
  ```

**Verifica.** Es la consulta 10 de `consultas_evidencia.sql`:

```bash
docker compose exec postgres psql -U etl -d dw_gestion \
  -c "SELECT serie_id, min(fecha) AS desde, max(fecha) AS hasta, count(*) AS obs, count(*) FILTER (WHERE es_nulo) AS nulas FROM staging.bcch_observacion GROUP BY 1 ORDER BY 1" \
  -c "SELECT s.serie_id, count(*) AS en_dw, round(min(f.valor),2) AS minimo, round(max(f.valor),2) AS maximo FROM dw.fact_indicador_economico f JOIN dw.dim_serie s USING (serie_key) GROUP BY 1 ORDER BY 1" \
  -c "SELECT task_id, estado, filas_leidas, filas_cargadas, filas_rechazadas, pct_nulos FROM audit.etl_audit WHERE dag_id='etl_bcch_indicadores' ORDER BY audit_id DESC LIMIT 8"
```

Qué esperar:

- Las 4 series parten cerca del **01-01-2024** y terminan cerca de **hoy**.
- La UF debería tener un valor por día calendario; el dólar y el euro, solo los días hábiles.
- La UTM, uno por mes.
- **Compruébalo tú** con los números y anótalos.

> Si una serie **no** parte en 2024, la API podría haber limitado el rango: recarga por años (2024, 2025, 2026) con 3 *Trigger w/ config*.

**Prueba de idempotencia:** vuelve a lanzar la **misma** recarga. Los conteos de staging y dw **no cambian**; raw sí suma filas, porque guarda cada extracción.

**Prueba del cruce.** Necesitas datos de Mercado Público en tu base, de la Issue 3. Es la consulta 9:

```sql
SELECT COALESCE(moneda_codigo,'(sin detalle)') AS moneda, count(*) AS licitaciones,
       count(monto_estimado_original) AS con_monto, count(monto_estimado_uf) AS convertidas_uf
FROM dw.fact_licitacion GROUP BY 1 ORDER BY 2 DESC;
```

Si `convertidas_uf = 0` es porque Mercado Público cargó **antes** que el BCCh. En la UI: DAG MP → último run → tarea `cargar_dw` → **Clear** (deja marcado **Downstream**). Así se recalculan los montos y se vuelven a disparar los KPIs.

**Prueba de aislamiento de fallas.** Es la misma idea de la demo, aplicada a tu DAG:

```bash
docker compose exec airflow-scheduler airflow variables set bcch_base_url https://si3.bcentral.cl/SieteRestWS/NO_EXISTE.ashx
docker compose exec airflow-scheduler airflow dags trigger etl_bcch_indicadores
#  validar_api falla (HTTP 404 o "Respuesta no JSON") y se reintenta; el DAG de MP sigue funcionando
docker compose exec airflow-scheduler airflow variables delete bcch_base_url
#  en la UI: Clear sobre validar_api (con Downstream) y el run se recupera
```

> ¿Por qué se cambia `bcch_base_url` y no `bcch_pass`? Porque `bcch_pass` viene de una **variable de entorno**, que **tiene prioridad**: cambiarla en la UI o con la CLI no tendría efecto. `bcch_base_url` no está en el entorno, así que la Variable de la base sí se usa.

### Paso 7 — Commit, push y Pull Request

```bash
cd ..
git add proyecto_etl/dags/etl_lib/bcch.py proyecto_etl/dags/dag_etl_bcch.py
git commit -m "feat(bcch): ETL de indicadores con recarga histórica y documentación de credenciales (#4)"
git push
```

Abre el PR con `Closes #4` y pide revisión a Jairo. En la descripción, pega los rangos y conteos por serie que obtuviste.

### Paso 8 — Fase 3 (mié 28-10): el BCCh va PRIMERO en la máquina de demo

Siguiendo el runbook del [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) (sección 9):

1. Activa el DAG del BCCh y lanza la **recarga histórica** antes de que Mercado Público haga su catchup. Así cada run de Mercado Público convierte los montos al cargar.
2. Al día siguiente:
   - Verifica que el **KPI4 tenga valor** (consulta 12).
   - Verifica que el cruce funcione (consultas 9 y 11).
   - Anota los números en la Issue 5.
3. Junto a Ivan, revisa la captura `03_grid_bcch.png`: debe mostrar los runs exitosos, incluida la recarga.

## 6. Criterios de aceptación (Definition of Done)

- [ ] Mis credenciales responden `Codigo 0` en el navegador (Paso 0)
- [ ] `airflow dags list-import-errors` muestra "No data found" y `validar_api` pasa
- [ ] La recarga histórica dejó las 4 series desde 2024 hasta hoy, en staging y en dw
- [ ] Repetir la misma recarga no cambia los conteos de staging ni de dw (idempotencia)
- [ ] El cruce funciona: hay montos en UF en `dw.fact_licitacion`
- [ ] La prueba de aislamiento de fallas funcionó y entiendo la precedencia de Variables
- [ ] Las mejoras 1 a 3 están aplicadas y la fecha de inicio está justificada con datos
- [ ] Puedo explicar cada fila de las tablas de los Pasos 2, 3 y 4

## 7. Tu parte en el informe y en la presentación

- **Informe:** escribe como comentario en la **Issue 5**:
  - (a) los rangos y conteos por serie;
  - (b) el resultado del cruce (% convertidas a UF);
  - (c) la historia "falla de credenciales (`-5`) → aislamiento → activación → recarga", para la sección 8.3.
  - Revisa las secciones 2 (fila del BCCh y campo de cruce), 4 y 8.3 del PDF.
- **Presentación:**
  - **Lámina 2, mitad "Fuente Banco Central y cruce"** (~30 s).
  - **Lámina 4 "Diseño de DAGs"** (~50 s): las 5 tareas, los schedules justificados, el Dataset OR y la recarga de rango.

## 8. Preguntas probables en la defensa (con respuesta guía)

1. **¿Por qué eligieron la API del Banco Central?** Es la fuente oficial de los indicadores económicos de Chile, su registro es gratuito y tiene series diarias confiables. Con ella podemos expresar los montos de las licitaciones en pesos y en UF, o sea, en términos reales, y comparar períodos.
2. **¿Qué series usan y por qué esas cuatro?** Son exactamente las monedas en que se publican las licitaciones: USD, EUR, UF (`CLF`) y UTM. Con ellas convertimos cualquier licitación a CLP y a UF.
3. **¿Qué es la UF y por qué convertir a UF?** Es una unidad reajustada por inflación. Un monto en UF es comparable entre meses o años porque no lo distorsiona el IPC; por eso el KPI4 se mide en miles de UF.
4. **¿Cuál es el campo de cruce entre las dos fuentes?**
   - **La fecha:** la dimensión de tiempo conformada.
   - **La moneda:** `dim_moneda.serie_id_bcch` indica qué serie convierte cada moneda.
   - Cada monto se convierte con el valor vigente a su fecha de publicación: el último disponible menor o igual a esa fecha.
5. **¿Qué pasa con los feriados y fines de semana?** El dólar y el euro no tienen valor esos días; si vienen como `NaN`, quedan con `es_nulo` y no pasan al hecho. La conversión toma el **último valor anterior** con un `JOIN LATERAL`, como se hace en la práctica.
6. **¿Para qué es la ventana de 7 días?** Para recoger publicaciones tardías o correcciones de días pasados. Como la carga es un upsert, volver a leer esos días no duplica nada.
7. **¿Por qué este DAG tiene `catchup=False` y el de Mercado Público `True`?** La API del BCCh entrega un rango completo en una llamada: el histórico es **un** run con `params`. La de Mercado Público entrega un día por llamada, y el catchup, que crea un run por día, encaja con eso.
8. **¿Cómo hicieron la recarga histórica?** Con *Trigger DAG w/ config* y `{"fecha_desde":"2024-01-01","fecha_hasta":"<hoy>"}`. Desde 2024, porque es el año de la licitación publicada más antigua de nuestro período. Repetirla no duplica.
9. **¿Qué pasó con el BCCh en el proyecto de referencia?** `validar_api` falló con `Codigo -5`, porque las credenciales no estaban activadas. Quedaron los RETRY y FAILED en la auditoría, pero **Mercado Público siguió cargando y los KPIs se calcularon**: fue una prueba real de aislamiento de fallas. La solución fue activar las credenciales y recargar el histórico.
10. **¿Cómo detectan un error si la API responde 200?** Revisando el campo `Codigo` de la respuesta: si es distinto de 0, se lanza `ApiError`. Sin esa revisión se cargaría una respuesta vacía como si fuera válida.
11. **¿Qué hace `lag()` y para qué sirve el filtro del 15 %?** `lag()` trae el valor del día anterior de la misma serie, y con él calculamos la variación %. Una variación diaria de más del 15 % en el dólar o la UF es casi seguro un error de dato, así que esa fila no pasa al hecho.
12. **¿Por qué la UTM no pasa por ese filtro?** Porque es mensual: compararla con un umbral pensado para variaciones diarias no tiene sentido.
13. **¿Qué valida `control_calidad`?** Que la UF esté entre 20.000 y 80.000 y el dólar entre 300 y 2.000. Si no, la tarea falla, no se emite el Dataset y los KPIs no usan esos valores.
14. **¿Qué pasa si la API del BCCh está caída varios días?**
    - El DAG falla y queda registrado.
    - El DAG de KPIs se sigue ejecutando cuando termina Mercado Público, porque usa la condición OR.
    - Las conversiones usan el último valor disponible.
    - `verificar_frescura` deja una advertencia si los datos tienen más de 3 días.
    - Al volver la API, la ventana de 7 días o una recarga con `params` completan lo faltante.
15. **¿Por qué no se puede cambiar `bcch_pass` desde la UI de Airflow?** Porque viene de la variable de entorno `AIRFLOW_VAR_BCCH_PASS`, y las Variables de entorno tienen prioridad sobre las de la base. Hay que cambiar `.env` y recrear los contenedores.
16. **¿Qué relación tiene la UTM con las licitaciones?** Los tramos de licitación pública se definen en UTM: L1 es menor a 100 UTM, LE va de 100 a 1.000 UTM y LP es mayor a 1.000 UTM. Además, algunas licitaciones expresan su monto en UTM y hay que convertirlo.

## 9. Si te atascas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `ApiError: BCCh serie ...: Codigo=-5 Invalid username or password` | Credenciales no activadas, correo o clave mal escritos, o `.env` desactualizado | Repite el Paso 0. Corrige `.env` y ejecuta `docker compose up -d` |
| Otro `Codigo` distinto de 0 | Serie mal escrita o fechas inválidas | Revisa el `serie_id` y que las fechas estén en formato `AAAA-MM-DD` |
| `Variable bcch_user does not exist` | Falta `BCCH_USER` en `.env` o no se recrearon los contenedores | Completa `.env` y ejecuta `docker compose up -d` |
| `control_calidad` falla | Valores fuera de rango: un dato erróneo o un cambio de unidad de la serie | Revisa con `SELECT ... min(valor), max(valor)` qué serie falla, antes de tocar el umbral |
| `ValueError: time data ... does not match format '%d-%m-%Y'` | Cambió el formato de fecha de la API | Mira un `payload` en `raw.bcch_series` y ajusta el formato |
| Los montos en UF siguen vacíos | Mercado Público se cargó antes que el BCCh | *Clear* de `cargar_dw` de MP con *Downstream* (Paso 6) |
| En PowerShell el `-c '{...}'` da error de JSON | PowerShell altera las comillas | Usa la UI (*Trigger DAG w/ config*) o WSL/Git Bash |

Si después de 30 minutos sigues bloqueado, comenta en esta issue con el error exacto (**sin tu clave**) y etiqueta `bloqueante`.
