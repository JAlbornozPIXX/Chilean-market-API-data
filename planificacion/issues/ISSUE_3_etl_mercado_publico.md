# Issue 3 · ETL Mercado Público (licitaciones) y demostración de falla en vivo

> **Responsable:** Jairo (@JAlbornozPIXX) · **Etiqueta:** `etl-mercadopublico` · **Hito:** ETL e integración
> **Rama:** `feature/3-etl-mercadopublico` · **Revisa tu PR:** Benja (@Sonnyxpj)
> **Depende de:** Issues 1 y 2 (PR A) en `main`, porque necesitas Docker Compose, `common.py` y las tablas.
> **Fechas:** ticket pedido **hoy, vie 23-10** · PR listo el **lun 26-10** · carga final nocturna el **mié 28-10** · ensayos de la demo del **jue 29-10** al **mié 04-11**

Documentos de apoyo: [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) · [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md) · [Manual de GitHub](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/02_MANUAL_GITHUB.md) · [Guía de presentación y defensa](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md)

---

## 1. Tu misión en una frase

Llevar al repositorio el **ETL de licitaciones de Mercado Público** y su DAG:

- **extraer:** llamar a la API y guardar en `raw`;
- **transformar:** limpiar y dejar en `staging`;
- **cargar:** pasar al modelo `dw`.

Debes probarlo con **tu propio ticket**, aplicar 2 mejoras, ejecutar la **carga final nocturna** y liderar la **demostración de falla en vivo** que exige la pauta.

## 2. ¿Por qué es importante? (lo que pide la pauta)

| Criterio de la pauta | Qué depende de ti |
|---|---|
| **Selección y justificación de fuentes** (5 pts): *"bien justificadas (viabilidad, documentación, límites, volumen)"* | Conocer a fondo la API de Mercado Público: endpoints, ticket, cuota y volumen |
| **Extracción y transformación** (5 pts): *"Extracción robusta (autenticación, paginación, límites de uso y errores HTTP) y transformaciones documentadas con validaciones de calidad; métricas de carga completas"* | `mercadopublico.py` completo |
| **Diseño de DAGs** (5 pts): *"Un DAG independiente por API ... schedule ... correctos y justificados"* y *"poder recargar un rango de fechas pasado"* | `dag_etl_mercadopublico.py` (schedule, catchup, params) |
| **Tolerancia a fallas** (5 pts): *"el resto del flujo continúa ante una falla y **se demuestra en vivo**"* | Tú diriges la demostración de falla controlada |

## 3. Conceptos que debes dominar

Están explicados en detalle en el [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md).

- **API REST y endpoint:**
  - Una API es una "puerta" para que un programa pida datos a otro sistema.
  - Un endpoint es una URL concreta de esa API. La nuestra es `.../licitaciones.json`.
- **Parámetros de consulta:** lo que va después de `?` en la URL (`?fecha=20102026&ticket=...`).
- **Ticket:** tu credencial personal de la API. Se envía en cada llamada y **nunca** se escribe en el código.
- **JSON:** formato de texto con llaves y listas en que responde la API.
- **Cuota y límites de uso (*rate limiting*):** máximo de consultas permitidas. En Mercado Público son **10.000 por día y por ticket**.
- **Paginación:** entregar los datos por partes. Aquí la "página" es **un día** de listado, más **lotes** de detalle.
- **Error de negocio con HTTP 200:** la API responde "OK" a nivel HTTP pero trae un `Codigo` de error en el cuerpo, por ejemplo **10500** = peticiones simultáneas.
- **Upsert, `ON CONFLICT` y `COALESCE`:**
  - Upsert significa insertar o, si la fila ya existe, actualizarla.
  - `COALESCE(a, b)` devuelve el primer valor que no sea nulo.
- **`DISTINCT ON` y `JOIN LATERAL`:**
  - `DISTINCT ON` se queda con una fila por grupo, la última extracción.
  - `JOIN LATERAL` es una subconsulta que se ejecuta "por cada fila" y busca el tipo de cambio vigente de esa licitación.
- **Normalización de texto:** quitar espacios extra y tildes, y pasar a mayúsculas para poder agrupar.
- **Campo calculado y recodificación:** `dias_publicacion_cierre` es un campo calculado. Convertir el código `8` en "Adjudicada" es una recodificación.
- **Airflow:**
  - **DAG** y **tarea**: el DAG es el flujo, las tareas son sus pasos.
  - **schedule cron** (`0 6 * * *`) y **data interval**: el período que procesa cada run.
  - **catchup** y **params / Trigger DAG w/ config**: sirven para las recargas.
  - **retries con backoff exponencial** y **execution_timeout**.
  - **max_active_runs**, **outlets / Dataset**.

## 4. Archivos que te pertenecen

```
proyecto_etl/dags/etl_lib/mercadopublico.py
proyecto_etl/dags/dag_etl_mercadopublico.py
```

**No toques:**

- `common.py`, que es de la Issue 1. Si necesitas un cambio, pídelo en la Issue 1.
- `01_modelo.sql` (Issue 2), `bcch.py` (Issue 4) y `kpis.sql` (Issue 5).

## 5. Paso a paso

### Paso 0 — Pide tu ticket (Día 0: ¡hoy, vie 23-10!)

1. Entra a https://www.chilecompra.cl/api/ → sección **"Pide tu ticket"**.
2. Acepta los términos de uso e **inicia sesión con Clave Única**.
3. Completa el formulario con tus datos reales. El ticket se genera automáticamente y llega a tu correo.

Reglas de uso, tomadas de los **términos de ChileCompra**:

- Hay un **único ticket por persona**.
- El límite es de **10.000 solicitudes diarias por ticket**, y no se puede modificar.
- Para descargas masivas se recomienda el horario de **22:00 a 07:00**.
- ChileCompra monitorea el uso **por dirección IP**.
- Al publicar los datos hay que **citar a la Dirección ChileCompra** como fuente.

> Nunca compartas tu ticket: ni por chat, ni en issues, ni en capturas de pantalla de URLs que lo contengan. Si 5 personas descargan masivamente desde la misma red (por ejemplo, la universidad), pueden aparecer bloqueos por IP. Para pruebas, usa poco detalle (Paso 6).

### Paso 1 — Explora la API a mano (15 min)

Hazlo antes de leer el código. Abre en el navegador (Firefox muestra el JSON ordenado):

1. **Listado de un día** (formato de fecha `ddmmaaaa`):
   `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha=20102026&ticket=TU_TICKET`
   Verás `Cantidad`, `FechaCreacion`, `Version` y `Listado` (una lista de licitaciones con `CodigoExterno`, `Nombre`, `CodigoEstado` y `FechaCierre`).
2. **Detalle de una licitación:** copia un `CodigoExterno` del listado y abre:
   `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?codigo=CODIGO&ticket=TU_TICKET`
   Trae mucho más:
   - `Comprador`, con `CodigoOrganismo`, `NombreOrganismo`, `RegionUnidad`, `ComunaUnidad` **y datos del funcionario** (nombre, RUT, cargo).
   - `Fechas`, con `FechaPublicacion`, `FechaCierre` y `FechaAdjudicacion`.
   - `Moneda`, `MontoEstimado`, `Tipo` e `Items.Cantidad`.

Anota tus respuestas a estas 3 preguntas, porque te servirán en la defensa:

- ¿Cuántas licitaciones trae el listado de un día?
- ¿Qué campos solo aparecen en el detalle?
- ¿Qué datos personales trae?

Los códigos de estado, según la documentación oficial, son: **5** Publicada, **6** Cerrada, **7** Desierta, **8** Adjudicada, **18** Revocada y **19** Suspendida.

### Paso 2 — Crea tu rama y copia los archivos

```bash
git switch main && git pull                    # main ya debe tener las Issues 1 y 2
git switch -c feature/3-etl-mercadopublico
git push -u origin feature/3-etl-mercadopublico
cp referencia/proyecto_etl/dags/etl_lib/mercadopublico.py proyecto_etl/dags/etl_lib/
cp referencia/proyecto_etl/dags/dag_etl_mercadopublico.py proyecto_etl/dags/
```

### Paso 3 — Entiende `mercadopublico.py`, función por función

Los números de línea son los del archivo de `referencia/`.

| Función (líneas) | Qué hace | Lo que debes poder explicar |
|---|---|---|
| Docstring (1–12) | Describe los 2 endpoints y la estrategia de "páginas" | El listado no trae paginación propia. Recorremos **un día por página** y el detalle **por lotes de códigos**, con tope |
| Constantes (27–31) | `DEFAULT_URL`, `ESTADOS` (5, 6, 7, 8, 18, 19) y `ERRORES_REINTENTABLES = {10500, 10501}` | 10500/10501 = peticiones simultáneas o límite de uso: conviene esperar y reintentar |
| `_cfg()` (34–37) | Lee las Variables `mp_base_url` (con valor por defecto), `mp_ticket` y `mp_max_detalle` (por defecto 150) | Toda la configuración está **fuera del código**. Cambiando `mp_base_url` hacemos la demo de falla |
| `_consultar()` (40–55) | Llama a la API con una pausa de 0,5 s. Si `Codigo` no viene, todo OK. Si es 10500/10501, espera 5·i s (5, 10, 15) y reintenta. Otro código lanza `ApiError` | Es el **2º nivel de reintento**, el de *negocio*. El 1º es el HTTP de `common.py` y el 3º lo hace Airflow |
| `rango_fechas()` (58–63) | Si el run trae `params` (`fecha_desde`, `fecha_hasta`), usa ese rango; si no, el día del `data_interval_start` | Así un mismo DAG sirve para la ejecución diaria y para la **recarga de un rango pasado** |
| `validar_api()` (66–74) | Pide el listado de **ayer** con un solo intento | Es la **validación previa** que pide la pauta: si la API o el ticket no responden, falla rápido y no gasta cuota en el resto |
| `extraer()` (77–114) | 1) recorre los días del rango y guarda **cada listado completo** en `raw.mp_licitaciones_dia`; 2) arma la lista de códigos; 3) descarta los que ya tienen detalle **y** están en un estado final (7, 8, 18); 4) toma hasta `mp_max_detalle` y descarga su detalle en `raw.mp_licitacion_detalle` | Si falla el detalle de una licitación, **se omite y se cuenta como rechazada**: la tarea sigue. Cada detalle se guarda en su propia transacción, así el avance no se pierde. La auditoría registra *"detalles descargados=X de Y"* |
| `_texto()` (117–122) | Quita espacios repetidos y saltos de línea; un texto vacío pasa a `None` | Normalización de textos |
| `_clave()` (125–130) | Mayúsculas y **sin tildes** (`unicodedata.normalize("NFKD")` + quitar marcas) | "Municipalidad de Concepción" pasa a "MUNICIPALIDAD DE CONCEPCION", para agrupar sin variantes |
| `_fecha()` (133–139) | Toma los 19 primeros caracteres ISO (`2026-10-20T15:30:00`) y los convierte a `datetime`; si son inválidos, `None` | Conversión de tipos |
| `_monto()` (142–147) | Convierte a `float`; un monto **0 o negativo** pasa a `None` ("no informado") | Tratamiento de nulos y atípicos |
| `transformar()` (150–238) | Lee **solo el raw de este run** (`run_id`). Arma un diccionario por `codigo_externo` (**deduplicación**). Completa con el detalle: organismo normalizado, región, comuna, fechas, moneda (por defecto CLP), monto, tipo e ítems. Calcula `dias_publicacion_cierre`, válido solo entre 0 y 365. Recodifica el estado; los desconocidos pasan a `-1`. Mide el % de nulos y hace upsert en staging | **No lee** el nombre, RUT ni cargo del funcionario, por **minimización de datos personales**. El `COALESCE` del upsert evita que un re-proceso que solo trae listado **borre** el detalle ya cargado (fue un bug real del proyecto). `tiene_detalle = t.tiene_detalle OR EXCLUDED.tiene_detalle` y `fecha_consulta = LEAST(...)` conservan la **primera** fecha de movimiento |
| `cargar_dw()` (241–298) | Asegura `dim_tiempo`, actualiza `dim_organismo` (upsert) y carga `fact_licitacion`. **Cruce con el BCCh:** con dos `JOIN LATERAL` busca el último valor de la moneda y de la UF con fecha ≤ la fecha de publicación | `monto_clp = monto × tipo de cambio` (1 si es CLP) y `monto_uf = monto_clp / valor UF`. "Último valor ≤ fecha" resuelve los feriados sin valor. El `pct_nulos` de esta tarea = % de montos que **no se pudieron convertir** |
| `control_calidad()` (301–309) | Falla si hay estados nulos o montos negativos | Si falla, **no se emite el Dataset** y los KPIs **no se recalculan** con datos malos |

### Paso 4 — Entiende el DAG `dag_etl_mercadopublico.py`

| Parámetro | Valor | Justificación (dila así en la defensa) |
|---|---|---|
| `dag_id` | `etl_mercadopublico_licitaciones` | Un DAG **independiente** por API, como pide la pauta |
| `schedule` | `"0 6 * * *"` | Todos los días a las 06:00 (hora de Chile). Las licitaciones se mueven **todos los días, incluso fines de semana**, y a esa hora el día anterior ya cerró |
| `start_date` | 2026-10-01 | Inicio del período de análisis |
| `catchup` | `True` | Al activar el DAG, Airflow crea **un run por cada día pendiente** desde `start_date`. Es la recarga histórica automática, y es segura porque todo es idempotente |
| `max_active_runs` | `1` | El ticket **no admite peticiones simultáneas** (error 10500): los runs van de a uno |
| `retries=3`, `retry_delay=2 min`, `retry_exponential_backoff=True`, `max_retry_delay=15 min` | — | Las fallas temporales (API caída, HTTP 429) se recuperan **solas**, esperando cada vez más |
| `execution_timeout=45 min` | — | La descarga de detalle tarda varios minutos. El límite evita tareas colgadas para siempre |
| `on_failure_callback`, `on_retry_callback` | de `common.py` | Registran FAILED/RETRY en `audit.etl_audit` y escriben `[ALERTA]` en el log |
| `params` `fecha_desde` / `fecha_hasta` | `Param(None, type=["null","string"], format="date")` | Permiten **recargar un rango** con *Trigger DAG w/ config* |
| `validar_api` con `retries=2` y `execution_timeout=2 min` | — | Si la API no responde, se descubre en ~2 minutos y no a mitad de la descarga |
| `control_calidad` con `outlets=[DS_MP]` | `Dataset(".../dw/fact_licitacion")` | Al terminar bien, "avisa" que `fact_licitacion` cambió y eso **dispara el DAG de KPIs** |
| `validar >> extraer >> transformar >> cargar >> calidad` | — | Orden obligatorio. Si una tarea falla, las siguientes quedan *upstream_failed* y no corren con datos incompletos |

**Tiempos reales de reintento.** En el log del proyecto de referencia, los 3 intentos de `validar_api` ocurrieron a las 02:12, 02:16 y 02:22. Recorrer todos los intentos tarda **unos 10 minutos**, por el backoff exponencial más una pequeña variación aleatoria. Esto importa para la demo (Paso 9).

### Paso 5 — Aplica las 2 mejoras

**Mejora 1 — Muestra aleatoria y reproducible del detalle.**

- **Problema:** por la cuota, solo se descarga el detalle de `mp_max_detalle` licitaciones por run. El código original toma **siempre las primeras** del listado. El orden en que la API entrega el listado **no está documentado**: si viniera, por ejemplo, ordenado por organismo, los KPIs que dependen del detalle (KPI3 a KPI6) quedarían **sesgados** hacia unos pocos organismos. En la referencia, el **92,2 %** de las licitaciones quedó sin detalle.
- **Solución:** barajar con una **semilla fija por rango de fechas**. Es aleatorio, pero **reproducible**: re-ejecutar el mismo día elige la misma muestra, así que el proceso sigue siendo idempotente.

En `extraer()`, cambia la línea 98:

```python
    pendientes = [c for c in dict(codigos).items() if c[0] not in finales][:max_det]
```

por:

```python
    pendientes = [c for c in dict(codigos).items() if c[0] not in finales]
    # Muestra aleatoria reproducible: el orden del listado no está documentado y tomar
    # siempre los primeros podría sesgar los KPIs; misma semilla = mismo resultado al re-ejecutar
    random.Random(f"{desde}:{hasta}").shuffle(pendientes)
    pendientes = pendientes[:max_det]
```

y agrega `import random` entre los `import` del inicio, en orden alfabético después de `import logging`. Este cambio ya se probó: compila, y la misma semilla da el mismo orden.

**Mejora 2 — Documentación actualizada.** La URL del docstring (`https://api.mercadopublico.cl/modules/api.aspx`) hoy **redirige** a https://www.chilecompra.cl/api/. Cambia las líneas 3 y 4 por:

```python
Documentación: https://www.chilecompra.cl/api/ (antes api.mercadopublico.cl/modules/api.aspx)
Requiere ticket (gratuito, uno por persona, 10.000 consultas diarias; se solicita con Clave Única),
```

**Decisión operativa: cuánto detalle descargar en la carga final.** La cuota alcanza con margen si se cumple:

```
llamadas por día ≈ runs × (1 validación + 1 listado + mp_max_detalle)  <  10.000
```

Con el catchup de octubre (~27 runs) y `mp_max_detalle = 250`, son 27 × 252 ≈ **6.800 llamadas**. La cobertura de detalle sube de ~8 % a cerca de un 25 %, y los KPIs 3 a 6 ganan representatividad. No cambia el código: es una Variable (Paso 8).

### Paso 6 — Prueba en tu computador

```bash
git fetch origin && git merge origin/main      # trae lo último de main a tu rama
cd proyecto_etl
cp .env.example .env                           # si aún no lo tienes; pon TU ticket en MP_TICKET
docker compose up -d

# 1) ¿Airflow puede leer tus DAGs sin errores?
docker compose exec airflow-scheduler airflow dags list-import-errors     # esperado: "No data found"

# 2) Menos detalle en pruebas: ahorra cuota y tiempo
docker compose exec airflow-scheduler airflow variables set mp_max_detalle 20

# 3) Prueba aislada de una tarea (no guarda estado en Airflow, pero sí llama a la API)
docker compose exec airflow-scheduler airflow tasks test etl_mercadopublico_licitaciones validar_api 2026-10-20
#    esperado en el log: "API Mercado Público disponible y ticket válido" y "Marking task as SUCCESS"

# 4) Activa el DAG: empieza el catchup (un run por día desde el 01-10, de a uno)
docker compose exec airflow-scheduler airflow dags unpause etl_mercadopublico_licitaciones
```

Mira cómo avanza en http://localhost:8080 → DAG → **Grid**. Con 20 detalles, cada run tarda alrededor de 1 minuto. Luego verifica los datos:

```bash
docker compose exec postgres psql -U etl -d dw_gestion \
  -c "SELECT count(*) AS listados FROM raw.mp_licitaciones_dia" \
  -c "SELECT count(*) AS licitaciones, count(*) FILTER (WHERE tiene_detalle) AS con_detalle FROM staging.mp_licitacion" \
  -c "SELECT estado, count(*) FROM staging.mp_licitacion GROUP BY 1 ORDER BY 2 DESC" \
  -c "SELECT count(*) AS en_dw FROM dw.fact_licitacion" \
  -c "SELECT task_id, estado, filas_leidas, filas_cargadas, filas_rechazadas, pct_nulos, round(duracion_seg,1) AS seg, left(mensaje,50) FROM audit.etl_audit WHERE dag_id='etl_mercadopublico_licitaciones' ORDER BY audit_id DESC LIMIT 10"
```

**Prueba de idempotencia:**

1. Anota los conteos de staging y dw.
2. En la UI, elige un run verde → **Clear** (todo el run) → espera a que vuelva a quedar verde.
3. Repite la consulta. **staging y dw no deben cambiar.** `raw` sí sube, a propósito.

**Prueba de recarga de un rango:**

1. En la UI usa ▶ → **Trigger DAG w/ config** → `{"fecha_desde": "2026-10-05", "fecha_hasta": "2026-10-06"}`.
2. Un solo run procesa los 2 días; mira el log de `extraer_raw`: aparecen 2 líneas "Listado ...".

**Revisa la muestra:**

```sql
SELECT count(DISTINCT codigo_organismo) AS organismos_con_detalle FROM staging.mp_licitacion WHERE tiene_detalle;
```

Con la muestra aleatoria deberías ver organismos variados.

**Ensaya la demo de falla.** Sigue el guion de la sección 3 de la [guía de presentación](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md).

### Paso 7 — Commit, push y Pull Request

```bash
cd ..
git status
git add proyecto_etl/dags/etl_lib/mercadopublico.py proyecto_etl/dags/dag_etl_mercadopublico.py
git commit -m "feat(mp): ETL de licitaciones con muestra aleatoria reproducible del detalle (#3)"
git push
```

Abre el PR con `Closes #3` y pide revisión a Benja. En la descripción, pega un extracto de tu prueba: conteos antes y después del *Clear* y el resultado de `list-import-errors`.

### Paso 8 — Carga final (mié 28-10, desde las 22:00, en la máquina de demo)

Junto a Ivan, siguiendo el runbook del [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) (sección 9):

1. **Primero** debe estar cargado el histórico del BCCh (Issue 4). Así las conversiones a UF quedan listas.
2. `airflow variables set mp_max_detalle 250` y luego `airflow dags unpause etl_mercadopublico_licitaciones`. Esto se hace en el horario nocturno que recomienda ChileCompra.
3. A la mañana siguiente:
   - Revisa que todos los runs estén verdes.
   - Ejecuta la consulta 8 de `consultas_evidencia.sql`, que da el % sin detalle.
   - Anota los números en la Issue 5.

### Paso 9 — Demostración de falla (dueño de la demo)

La pauta pide mostrar **los reintentos, que el resto del flujo continúa y cómo se recupera**. Como recorrer todos los reintentos tarda ~10 minutos y la presentación dura 8, **la falla se dispara al comenzar la presentación** y se muestra en la lámina 8.

El guion completo, con comandos, tiempos, frases y plan B, está en la sección 3 de la [guía de presentación](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md). Ensáyalo **al menos 3 veces** con cronómetro, junto a Ivan, que opera el teclado.

## 6. Criterios de aceptación (Definition of Done)

- [ ] `airflow dags list-import-errors` muestra "No data found"
- [ ] `validar_api` pasa con mi ticket (`airflow tasks test`)
- [ ] El catchup de prueba termina en verde y hay datos en raw, staging y dw
- [ ] La prueba de idempotencia da los mismos conteos en staging y dw
- [ ] La recarga de rango con *Trigger DAG w/ config* procesa los días pedidos
- [ ] Las mejoras 1 y 2 están aplicadas, y el código compila (el DAG aparece en la UI sin errores)
- [ ] Ensayé la demo de falla de principio a fin al menos una vez
- [ ] Puedo explicar cada fila de las tablas de los Pasos 3 y 4, y los 3 niveles de reintento

## 7. Tu parte en el informe y en la presentación

- **Informe:** escribe como comentario en la **Issue 5**:
  - (a) los números de la carga final de MP;
  - (b) un párrafo sobre la muestra aleatoria;
  - (c) los hallazgos nuevos que observes.
  - Revisa en el PDF las secciones 2 (fila de Mercado Público), 3.1, 3.2 y 8.
- **Presentación:**
  - **Lámina 2, mitad "Fuente Mercado Público"** (~30 s).
  - **Lámina 8 "Tolerancia a fallas: demo en vivo"** (~85 s, narras mientras Ivan opera).

## 8. Preguntas probables en la defensa (con respuesta guía)

1. **¿Por qué eligieron Mercado Público?**
   - Es la fuente oficial de las compras públicas: más de 1.000 entidades del Estado.
   - Tiene una API gratuita y documentada, se actualiza a diario y su volumen es manejable: cerca de 900 licitaciones con movimiento por día.
   - Permite medir eficiencia, competencia y transparencia.
2. **¿Qué endpoints usan?** El listado por fecha (`?fecha=ddmmaaaa`), que trae un resumen de todas las licitaciones con movimiento ese día, y el detalle por código (`?codigo=`), con organismo, fechas, moneda, monto e ítems.
3. **¿Cómo se autentican?** Con un ticket personal obtenido con Clave Única. Llega como la Variable `mp_ticket` desde `.env`. No está en el código y, como los errores se registran sin los parámetros de la URL, tampoco aparece en los logs.
4. **¿Qué límites tiene la API y cómo los respetan?**
   - Los límites son 10.000 solicitudes diarias por ticket, el error 10500 por peticiones simultáneas y los HTTP 429.
   - Los respetamos así:
     - `max_active_runs=1`;
     - pausa de 0,5 s entre llamadas;
     - reintentos HTTP con backoff y `Retry-After`;
     - reintento de negocio con espera 5·i;
     - tope `mp_max_detalle`;
     - carga masiva de noche.
5. **¿Dónde está la paginación?** Recorremos el rango **día por día**: cada día es una página del listado. El detalle se descarga **por lotes** de códigos con tope, y lo pendiente se completa en los runs siguientes.
6. **¿Qué pasa si la API responde 200 pero con error?** `_consultar` revisa el campo `Codigo`. Si es 10500 o 10501 espera y reintenta; si es otro, lanza `ApiError` y la tarea falla con un mensaje claro.
7. **¿Y si falla el detalle de una sola licitación?** Se omite, se cuenta como rechazada en la auditoría y la tarea continúa. Esa licitación queda sin detalle y se intenta de nuevo en un run posterior.
8. **¿Qué transformaciones aplican?** Las 9 de la pauta:
   - renombrar campos: `CodigoExterno` pasa a `codigo_externo`;
   - convertir tipos: fechas y números;
   - normalizar textos: espacios, mayúsculas, tildes;
   - tratar nulos y atípicos: monto 0 pasa a NULL; plazos negativos o mayores a 365 días pasan a NULL;
   - deduplicar por código;
   - calcular campos: los días de publicación;
   - recodificar estados: códigos a texto, desconocidos a -1;
   - filtrar: se excluyen los datos personales;
   - validar calidad: `control_calidad`.
9. **¿Por qué usan COALESCE en el upsert?** Fue un problema real. Al re-procesar un día que solo traía listado, el upsert sobrescribía con NULL los campos del detalle ya cargado. Con `COALESCE(nuevo, existente)` se conserva lo que ya teníamos.
10. **¿Por qué el tiempo del hecho es la fecha de movimiento y no la de publicación?** El listado diario trae toda licitación **con movimiento** ese día, incluso algunas publicadas en 2024. Con la fecha de publicación, los KPIs mensuales quedaban dispersos en meses con 1 o 2 registros.
11. **¿Cómo convierten los montos a UF?** Con un `JOIN LATERAL` que busca el último valor de la moneda (y de la UF) con fecha menor o igual a la de publicación. CLP = monto × tipo de cambio; UF = CLP / UF. "El último valor ≤ fecha" cubre feriados y fines de semana.
12. **¿Cómo recargan un rango pasado?** De tres formas:
    - con el **catchup**, que es automático desde `start_date`;
    - con **Trigger DAG w/ config** `{"fecha_desde": ..., "fecha_hasta": ...}`;
    - con `airflow dags backfill -s ... -e ...`.

    Todo es idempotente.
13. **¿Por qué ese schedule?** Mercado Público cambia todos los días, incluidos fines de semana. A las 06:00 el día anterior está completo y es un horario de bajo uso.
14. **¿Por qué una muestra aleatoria con semilla?** Tomar siempre los primeros del listado podría sesgar los KPIs, porque el orden no está documentado. La semilla por fecha la hace reproducible: el mismo día da la misma muestra, lo que mantiene la idempotencia.
15. **¿Qué pasa si se cae la API de Mercado Público?**
    1. `validar_api` falla y Airflow la reintenta con espera creciente.
    2. Cada intento queda en `etl_audit` (RETRY y luego FAILED) y en el log con `[ALERTA]`.
    3. El DAG del BCCh y el de KPIs siguen funcionando: son independientes y el de KPIs usa un Dataset con OR.
    4. Cuando la API vuelve, se hace *Clear* o se espera al siguiente run, y el catchup recupera los días pendientes.
16. **¿Qué datos personales trae la API y qué hacen con ellos?** El detalle trae nombre, RUT y cargo del funcionario comprador. Por minimización no los leemos al transformar, así que no llegan a staging ni a dw. Solo quedan en el JSON crudo, y el rol de analista no tiene acceso a `raw`.
17. **¿Qué valida `control_calidad` y qué pasa si falla?** Valida que no haya estados nulos ni montos negativos. Si falla, la tarea queda roja, no se emite el Dataset y los KPIs no se recalculan con datos malos.
18. **¿Cuántas llamadas hace un run?** 1 de validación + 1 por cada día del rango + hasta `mp_max_detalle` detalles. Un run diario con 150 detalles hace unas 152.

## 9. Si te atascas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `ApiError: Mercado Público Codigo=10500` repetido | Otro proceso usa tu ticket al mismo tiempo, por ejemplo dos computadores con el mismo ticket | Usa un ticket por persona; espera unos minutos; revisa `max_active_runs=1` |
| `HTTP 429` | Muchas peticiones seguidas | Los reintentos lo absorben. Si persiste, baja `mp_max_detalle` y espera |
| `KeyError: 'Variable mp_ticket does not exist'` | `.env` sin `MP_TICKET`, o contenedores creados antes de editarlo | Completa `.env` y ejecuta `docker compose up -d` |
| El DAG no aparece en la UI | Error de importación | `airflow dags list-import-errors` muestra el archivo y la línea |
| Los runs quedan *queued* para siempre | El DAG está pausado o el scheduler está caído | Activa el interruptor del DAG y revisa `docker compose ps` |
| El catchup demora demasiado | `mp_max_detalle` alto | Bájalo a 20 para pruebas: `airflow variables set mp_max_detalle 20` |
| `monto_estimado_uf` vacío en `fact_licitacion` | El BCCh no tiene datos para esas fechas | Carga el histórico del BCCh (Issue 4) y haz *Clear* de `cargar_dw` con *Downstream* |
| Cambié `mp_ticket` en la UI y no pasa nada | La Variable de entorno (`AIRFLOW_VAR_MP_TICKET`) **tiene prioridad** sobre la de la UI | Cambia `.env` y ejecuta `docker compose up -d` |

Si después de 30 minutos sigues bloqueado, comenta en esta issue con el error exacto (**sin tu ticket**) y etiqueta `bloqueante`.
