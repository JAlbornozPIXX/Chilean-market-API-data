# Guía de presentación (8 minutos), demostración de falla y defensa individual

> La Parte 2 vale el **70 % de la nota**: 30 % la presentación grupal y **40 % las preguntas individuales**.
> **Quien no se presenta obtiene 1,0.** La exposición dura 8 minutos y después cada integrante recibe preguntas.

## 1. Qué evalúa la pauta en esta parte

| Criterio | "Destacado" (5) significa… |
|---|---|
| Presentación grupal (30 %) | *"Presentación clara, estructurada y bien organizada, apoyada en una presentación PowerPoint; exposición fluida y **seguridad al mostrar los DAGs en Airflow, el ETL y los indicadores**"*. Además, la modalidad exige que expongan **todos** los integrantes e incluya la **demostración de falla controlada** |
| Preguntas individuales (40 %) | *"Cada integrante responde con claridad, precisión técnica y **dominio total del proceso**"*. Las preguntas pueden ser de **cualquier** parte, no solo de la tuya |

## 2. Estructura de la exposición: 10 láminas y 5 expositores

Las láminas salen de `docs/generar_entregables.py` (Issue 5). Los tiempos suman **7:40**, lo que deja 20 segundos de margen.

| # | Lámina | Expositor | Tiempo | Qué decir (idea central) | Mostrar en vivo |
|---|---|---|---|---|---|
| 1 | Portada | Sofia | 0:10 | Equipo y pregunta de gestión | — (Ivan dispara la falla en silencio, ver sección 3) |
| 2 | Problema y fuentes | Jairo (MP) → Benja (BCCh) | 0:30 + 0:30 | MP: qué datos trae, ticket y cuota de 10.000 por día. BCCh: series, credenciales y **campo de cruce** fecha + moneda | — |
| 3 | Arquitectura | Ivan | 0:50 | Docker Compose; Airflow con LocalExecutor; PostgreSQL por capas; configuración en Connections y Variables **sin claves en el código** | La lista de DAGs (5 s) |
| 4 | Diseño de DAGs | Benja | 0:50 | Las 5 tareas; schedules justificados; catchup vs. `params`; Dataset OR; reintentos | El *Graph* de un DAG; Ivan lanza el DAG del BCCh |
| 5 | Modelo de datos | Gustavo | 0:50 | Constelación de 2 hechos, grano, `dim_tiempo` conformada e idempotencia | (Opcional) la consulta 2 o la 9 |
| 6 | Indicadores de gestión | Sofia | 0:55 | Los 6 KPIs contra su meta y qué decisión apoya cada uno | La tabla `mart.kpi_mensual` (opcional) |
| 7 | Hallazgos de la carga | Gustavo | 0:30 | La fecha de movimiento como grano, el monto oculto (KPI5), el error con HTTP 200 y COALESCE | — |
| 8 | **Tolerancia a fallas: DEMO EN VIVO** | Jairo narra, Ivan opera | 1:25 | Reintentos, aislamiento y recuperación (sección 3) | Grid, log, auditoría y *Clear* |
| 9 | Estrategia de datos | Sofia | 0:40 | Usuarios y decisiones, calidad medida, linaje, riesgos y escalabilidad | — |
| 10 | Lecciones aprendidas y cierre | Ivan | 0:30 | 4 lecciones y una frase de cierre; ofrecer la palabra para preguntas | Mostrar que el run recuperado quedó **verde** |

**Transiciones.** Cada expositor cierra presentando al siguiente, por ejemplo: *"Con los datos en la base, mi compañera explicará el modelo dimensional."* Así la exposición se ve fluida y ordenada.

**Reglas al exponer:**

- No leas la lámina: explica la idea y apunta a la evidencia.
- Habla a la comisión, no a la pantalla.
- Mantén siempre un vocabulario preciso: "upsert", "data interval", "Dataset".
- Si algo en vivo tarda, sigue con la siguiente lámina y vuelve después; no esperes en silencio.

## 3. La demostración de falla en vivo (guion completo)

La pauta pide: *"el grupo provoca una falla controlada en una tarea (por ejemplo, una URL incorrecta) y muestra **los reintentos**, que **el resto del flujo continúa** y **cómo se recupera**"*.

**El problema del tiempo:** con el backoff exponencial, recorrer todos los intentos tardó **unos 10 minutos** en las pruebas (intentos a las 02:12, 02:16 y 02:22 en el log). Por eso **la falla se dispara al comenzar** y se explica en la lámina 8, cuando ya hay reintentos registrados.

### 3.1 Preparación: 60 a 30 minutos antes, en la máquina de demo

```bash
cd <ruta>/Chilean-market-API-data/proyecto_etl
docker compose up -d && docker compose ps -a        # todo Up / healthy; airflow-init Exited (0)
docker compose exec airflow-scheduler airflow variables set mp_max_detalle 10   # la recuperación termina en ~1 min
```

En la UI, verifica lo siguiente:

- Los **3 DAGs activos**.
- Los últimos runs en **verde**.
- **Ningún run de MP en ejecución**: con `max_active_runs=1`, un run en curso dejaría la demo en cola.

Deja abiertas estas pestañas, con zoom al 125–150 % para el proyector:

1. Grid del DAG de MP.
2. Grid del DAG del BCCh.
3. Grid del DAG de KPIs.
4. La vista *Datasets*.
5. Una terminal con los comandos de abajo **ya escritos**.
6. DBeaver o psql con la consulta 4 de `consultas_evidencia.sql` (fallas y reintentos) lista para ejecutar.

Además:

- Revisa que tienes internet y deja el **hotspot del celular** preparado.
- Apaga las notificaciones.
- Ten el cargador conectado.
- Ten el **video de respaldo** (sección 3.4) en el escritorio.

### 3.2 Guion minuto a minuto

| Momento | Quién | Acción |
|---|---|---|
| **0:00** (lámina 1, mientras Sofia abre) | Ivan, en silencio | Ejecuta los 2 comandos de abajo. El 1.er intento de `validar_api` falla en segundos y queda **naranjo** (*up_for_retry*) |
| **~2:20** (lámina 4) | Ivan | Lanza el DAG del BCCh: `airflow dags trigger etl_bcch_indicadores`. Corre normal y, al terminar, dispara el DAG de KPIs |
| **~4:40** (lámina 8) | Jairo narra, Ivan muestra | Ver la tabla de la sección 3.3 |
| **~6:05** (láminas 9 y 10) | Ivan | Si el run recuperado no ha terminado, se muestra en verde al cierre de la lámina 10 o al empezar las preguntas |

Comandos (copiar y pegar). La URL incorrecta es la misma que se usó en las pruebas:

```bash
docker compose exec airflow-scheduler airflow variables set mp_base_url https://api.mercadopublico.cl/servicios/v1/publico/URL_INCORRECTA.json
docker compose exec airflow-scheduler airflow dags trigger etl_mercadopublico_licitaciones
```

### 3.3 Lámina 8: qué mostrar y qué decir (85 segundos)

| Paso | Ivan muestra | Jairo dice (idea) |
|---|---|---|
| 1. La falla | Grid del DAG de MP: `validar_api` naranjo o rojo, y las tareas siguientes sin ejecutar | *"Al inicio de la presentación cambiamos la Variable `mp_base_url` por una URL incorrecta. La validación previa detectó el error y no se gastó cuota descargando nada."* |
| 2. Los reintentos | Log del intento: `HTTP 404 ... URL_INCORRECTA.json` y la línea `[ALERTA] ... estado=RETRY intento=N` | *"Airflow reintenta solo, con espera exponencial: 2 minutos, luego más. Cada intento lo registra nuestro `on_retry_callback`."* |
| 3. La auditoría | Consulta 4: filas RETRY (y FAILED si ya se agotaron los intentos) | *"Todo queda en `audit.etl_audit`: tarea, intento, hora y mensaje del error."* |
| 4. El resto del flujo continúa | Grid del BCCh con su run verde de hace un momento, y Grid de KPIs con su run disparado por el Dataset del BCCh | *"Los DAGs son independientes: el del Banco Central siguió funcionando y, gracias a la condición OR de los Datasets, los KPIs se recalcularon igual."* |
| 5. La recuperación | En la terminal: `airflow variables delete mp_base_url`. En la UI: clic en `validar_api` → **Clear**, con *Downstream* marcado → confirmar. La tarea pasa a verde en segundos y las siguientes empiezan a correr | *"Corregimos la causa restaurando la URL por defecto y limpiamos la tarea. El run se recupera desde donde falló, y como la carga es idempotente, no se duplica nada."* |

Comando de recuperación:

```bash
docker compose exec airflow-scheduler airflow variables delete mp_base_url
```

### 3.4 Plan B: si internet o la API fallan durante la presentación

- **Graben un video** de la demo completa el **mar 03-11**, de 1:30 como máximo, con la misma narración. Si en vivo falla la red o la API, Jairo dice *"tenemos la grabación de esta misma demo de ayer"* y la reproduce.
- **Respaldo estático:** la captura `07_grid_recuperado.png` y `log_falla.txt`, que están en el informe.
- **Nunca** improvisen cambios de código durante la presentación.

### 3.5 Después de presentar

```bash
docker compose exec airflow-scheduler airflow variables set mp_max_detalle 150
docker compose exec airflow-scheduler airflow variables delete mp_base_url    # por si quedó definida
```

## 4. Checklist de la máquina de demo (mié 04-11 y el mismo jue 05-11)

- [ ] Laptop cargada, cargador y adaptador HDMI o USB-C; resolución y zoom probados en un proyector
- [ ] Docker Desktop abre bien; `docker compose ps -a` sin problemas
- [ ] Airflow: los 3 DAGs activos y los runs de los últimos días en verde. Si la laptop estuvo apagada, el catchup de MP recupera los días faltantes: hay que dejarla encendida al menos 1 hora antes
- [ ] `mp_max_detalle = 10` y ningún run de MP en curso
- [ ] Las pestañas de la sección 3.1 abiertas
- [ ] La terminal con los comandos de la demo preparados
- [ ] El PPTX abierto en modo presentación, con una copia en un pendrive y otra en la nube
- [ ] El video de respaldo en el escritorio
- [ ] Hotspot del celular probado
- [ ] Los respaldos `.dump` en el Drive del grupo, por si hay que usar la máquina de reserva

## 5. Capacitación cruzada y simulacro de preguntas

| Sesión | Fecha | Dinámica | Resultado esperado |
|---|---|---|---|
| Capacitación cruzada 1 | mar 27-10 (90 min) | Cada integrante explica su parte en 12 min **mostrando el código**. Los demás le hacen 2 preguntas de su issue | Todos conocen el flujo completo |
| Capacitación cruzada 2 (rotación) | lun 02-11 | Cada uno explica una parte que **no** es suya: Ivan, el ETL de MP (de Jairo); Gustavo, el ETL del BCCh (de Benja); Jairo, los KPIs y el informe (de Sofia); Benja, la infraestructura (de Ivan); Sofia, el modelo de datos (de Gustavo) | Cubrir los vacíos antes del simulacro |
| Simulacro | mar 03-11 | 6 preguntas al azar para cada uno: 2 de su parte y 4 de otras, de este banco y de las issues. Máximo 1 minuto por respuesta | Cada uno califica a los demás con la rúbrica de la pauta: **5** claro, preciso y con dominio total; **4** adecuado; **3** incompleto; **1** no responde. Quien saque menos de 4 repasa esa parte con su dueño |
| Ensayo general | mié 04-11 | La exposición completa con cronómetro y con la demo en vivo, más 10 minutos de preguntas | 8 minutos o menos y la demo sin sorpresas |

**Cómo responder bien.** Usa una estructura en 4 tiempos:

1. **Respuesta directa** en una frase.
2. **Cómo lo hicimos:** el archivo, la función o la tabla.
3. **Por qué:** la decisión de diseño.
4. **Evidencia:** un número, una captura o una consulta.

Si no sabes un detalle, dilo con honestidad y conéctalo con lo que sí sabes: *"No recuerdo el valor exacto, pero lo calculamos en `kpis.sql` con…"*. Nunca inventes.

> **Ejemplo.** *"¿Cómo evitan duplicar datos si un DAG se ejecuta dos veces?"*
> → *"Las cargas son idempotentes. En staging y dw usamos `INSERT ... ON CONFLICT DO UPDATE` sobre la clave natural, y el mart se recalcula con DELETE + INSERT en una sola transacción. Lo comprobamos haciendo Clear de un run ya cargado: los conteos de staging y dw no cambiaron (consulta 2 del Anexo A)."*

## 6. Banco de preguntas transversales (todos deben poder responderlas)

Las preguntas específicas de cada parte están en la sección "Preguntas probables" de cada issue.

**Visión general**

1. **Explica el flujo completo en 30 segundos.**
   1. Dos DAGs de Airflow extraen datos de las APIs de Mercado Público y del Banco Central.
   2. Guardan el JSON crudo en `raw`, lo limpian en `staging` y lo cargan en un modelo dimensional en `dw`.
   3. Al terminar bien, cada DAG emite un Dataset que dispara un tercer DAG, que calcula 6 KPIs en SQL en `mart`.
   4. Todo queda auditado en `audit.etl_audit`, y la configuración y las claves viven fuera del código.
2. **¿Por qué Airflow y no un script con cron?** Airflow da dependencias entre tareas, reintentos con espera, timeouts, callbacks de falla, recarga de fechas pasadas (catchup y params), una UI para ver cada ejecución y su log, y disparo por datos (Datasets). Con cron tendríamos que programar todo eso a mano.
3. **¿Por qué PostgreSQL?** Lo exige la pauta y además es robusto. Tiene `JSONB` para el dato crudo, `ON CONFLICT` para la idempotencia, funciones de ventana para los KPIs y transacciones.
4. **¿Qué es un DAG?** Un flujo de tareas con dependencias en una sola dirección y sin ciclos. Por ejemplo: validar → extraer → transformar → cargar → controlar calidad.
5. **¿Qué es la idempotencia y cómo la prueban?** Ejecutar 1 o N veces deja el mismo resultado. La logramos con `ON CONFLICT` y con DELETE + INSERT transaccional, y la probamos haciendo *Clear* de un run ya cargado: los conteos no cambiaron.
6. **¿Diferencia entre OLTP y OLAP?** El OLTP opera (transacciones, el estado actual): es el sistema de ChileCompra. El OLAP analiza (agregaciones, historia, modelo dimensional): son nuestros `dw` y `mart`.

**Fuentes y extracción**

7. **¿Cuál es el campo de cruce?** La fecha (`dim_tiempo`) y la moneda (`dim_moneda.serie_id_bcch`). Cada monto se convierte con el valor del BCCh vigente a su fecha de publicación.
8. **¿Cómo manejan los límites de las APIs?** Con `max_active_runs=1`, pausas entre llamadas, reintentos HTTP con backoff y `Retry-After`, reintento de negocio ante el error 10500, un tope de detalle (`mp_max_detalle`) y cargas masivas de noche.
9. **¿Qué pasa si una API responde 200 pero con error?** Revisamos el `Codigo` del cuerpo (en el BCCh debe ser 0; en MP no debe venir). Si hay error, lanzamos `ApiError` y la tarea falla con un mensaje claro.
10. **¿Por qué guardan el JSON crudo?** Lo pide la pauta, permite re-procesar sin volver a llamar a la API (cuidando la cuota) y deja trazabilidad de qué llegó y cuándo.
11. **¿Dónde está la paginación?** En MP se recorre un día por página y el detalle por lotes. En el BCCh se hace una llamada por serie para todo el rango.

**Transformación, calidad y modelo**

12. **¿Qué transformaciones hicieron?**
    - Renombrar campos.
    - Convertir tipos.
    - Normalizar textos.
    - Tratar nulos y atípicos.
    - Deduplicar.
    - Crear campos calculados.
    - Recodificar estados.
    - Filtrar datos personales.
    - Validar la calidad.

    Son las 9 de la pauta.
13. **¿Cómo saben que los datos son de calidad?**
    - Medimos el % de nulos en cada carga (en la auditoría).
    - Marcamos `es_nulo`.
    - Descartamos variaciones mayores al 15 %.
    - `control_calidad` valida rangos, estados y montos.
    - Si algo falla, no se emite el Dataset y los KPIs no usan datos malos.
14. **¿Cuál es su modelo dimensional?** Una constelación: `fact_licitacion` (grano: licitación) y `fact_indicador_economico` (grano: serie × día), que comparten `dim_tiempo`. Las demás dimensiones son organismo, estado, moneda y serie.
15. **¿Qué es un upsert?** Insertar, o actualizar si la clave ya existe: `INSERT ... ON CONFLICT DO UPDATE`.

**Airflow y tolerancia a fallas**

16. **¿Qué pasa si se cae una API?**
    - Su DAG reintenta con espera creciente, registra cada intento y, si se agotan los intentos, queda FAILED con su `[ALERTA]`.
    - El otro DAG sigue funcionando, y los KPIs se recalculan por la condición OR.
    - Cuando la API vuelve, se hace *Clear* o el catchup recupera lo faltante.
17. **¿Qué diferencia hay entre los reintentos de requests y los de Airflow?**
    - Los de requests (urllib3) son inmediatos, en segundos y dentro de la misma llamada.
    - Los de negocio (`_consultar`) responden a códigos de error de la API.
    - Los de Airflow reejecutan la **tarea completa**, minutos después.
18. **¿Qué hace `validar_api`?** Comprueba **antes de extraer** que la API responde y que la credencial es válida. Si falla, el error aparece en segundos y no se gasta cuota.
19. **¿Qué es el catchup?** Al activar un DAG, Airflow crea los runs de todos los intervalos pendientes desde `start_date`. Lo usamos en MP para la carga histórica diaria.
20. **¿Cómo recargan los datos de hace dos semanas?** Con *Trigger DAG w/ config* y `{"fecha_desde": "...", "fecha_hasta": "..."}`, o con `airflow dags backfill`. Como es idempotente, no duplica.
21. **¿Qué es el *data interval*?** El período que cubre un run. El run diario de MP de las 06:00 procesa el día anterior completo.
22. **¿Por qué esos schedules?** MP se mueve todos los días, incluso los fines de semana: corre a diario a las 06:00. El BCCh publica en días hábiles: corre de lunes a viernes a las 10:00, con una ventana de 7 días. Los KPIs no tienen horario: corren cuando llegan datos nuevos.
23. **¿Qué es un Dataset en Airflow?** Una representación de un dato, como nuestras tablas de hechos. La tarea que lo actualiza lo declara en `outlets`, y el DAG que lo consume lo pone en su `schedule`. En Airflow 3 se llama *Asset*.
24. **¿Qué registra la auditoría?** Por cada tarea: DAG, tarea, run, intento, filas leídas, cargadas y rechazadas, % de nulos, duración, estado y mensaje. Las fallas y los reintentos los escriben los callbacks.
25. **¿Qué pasa si `control_calidad` falla?** La tarea queda roja, no se emite el Dataset y los KPIs **no** se recalculan con esos datos; queda el registro en la auditoría.

**Indicadores y estrategia**

26. **¿Qué KPI consideras más importante y por qué?** Respuesta personal, pero argumentada. Por ejemplo, el KPI2 (desiertas): un proceso desierto hace perder tiempo y obliga a repetir la compra. Su valor fue de X % frente a una meta de 15 %.
27. **¿Quién usaría estos KPIs?** Las jefaturas de abastecimiento (KPI1–3), ChileCompra (KPI5–6), DIPRES y finanzas (KPI4), y Contraloría y la ciudadanía como control social.
28. **¿Qué limitaciones tienen?** KPI3 a KPI6 se calculan sobre una muestra del detalle, limitada por la cuota (aleatoria y reproducible). El mes en curso es parcial. Muchas licitaciones ocultan su monto.
29. **¿Qué es la UF y por qué la usan?** Una unidad reajustada por inflación. Con ella los montos de distintos meses son comparables en términos reales.
30. **¿Cómo crecería la solución?**
    - **Más fuentes:** un DAG por API, que reutiliza `common.py`, y un Dataset más en la condición OR.
    - **Más volumen:** más tickets, particionar los hechos o un executor distribuido.
    - **Más KPIs:** nuevas consultas en `kpis.sql`.
31. **¿Qué riesgos tiene?** Cambios en las APIs, límites de uso, credenciales, licencias y datos personales, cada uno con su mitigación (matriz de riesgos del informe, sección 6.5).

**Seguridad, reproducibilidad y trabajo en equipo**

32. **¿Dónde están las credenciales?**
    - En el archivo `.env`, que no se versiona ni se entrega.
    - Docker Compose las convierte en `AIRFLOW_CONN_*` y `AIRFLOW_VAR_*`, y el código solo las pide por su nombre.
    - El .zip se genera con `git archive`, que no incluye `.env`.
33. **¿Qué datos personales maneja la solución?** El detalle de MP trae el nombre, RUT y cargo del funcionario comprador. No los cargamos a staging ni a dw (minimización), y el rol de analista no puede leer `raw`.
34. **¿Cómo se reproduce la solución en otra PC?** Se instala Docker, se copia `.env.example` a `.env` con las credenciales y se ejecuta `docker compose up -d`: el DDL se aplica solo. Lo probamos en una segunda PC siguiendo solo el README.
35. **¿Cómo trabajaron en equipo sin pisarse?** Con GitHub: una issue por integrante, una rama por issue, Pull Requests con revisión obligatoria y *squash merge*. Cada archivo tiene un dueño, así que no hubo conflictos de código.
36. **¿Qué fue lo más difícil y qué aprendieron?** Por ejemplo:
    - Las APIs informan errores con HTTP 200: hay que validar el cuerpo.
    - El BCCh requería **activar** las credenciales.
    - Re-procesar borraba el detalle, y se resolvió con `COALESCE`.
    - El grano temporal tuvo que ser la fecha de movimiento.
