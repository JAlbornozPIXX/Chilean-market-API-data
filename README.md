# Chilean-market-API-data — INF-522 Evaluación 2

Solución de analítica de datos para medir la **gestión de las compras públicas del Estado de Chile**:

- **Procesos ETL en Python** orquestados con **Apache Airflow 2.10**.
- Consumen dos APIs públicas chilenas: **Mercado Público (ChileCompra)** y **Banco Central de Chile**.
- Cargan un **modelo dimensional en PostgreSQL** por capas (`raw → staging → dw → mart`, más `audit`).
- Calculan **6 indicadores de gestión (KPIs)**.

## Equipo y asignación

| Issue | Parte del proyecto | Rol | Integrante | Usuario GitHub |
|---|---|---|---|---|
| [#1](planificacion/issues/ISSUE_1_infraestructura.md) | Infraestructura, configuración segura y librería común | Plataforma y máquina de demo | Ivan Sanchez | [@ivvan-asv](https://github.com/ivvan-asv) |
| [#2](planificacion/issues/ISSUE_2_modelo_de_datos.md) | Modelo de datos en PostgreSQL, idempotencia y auditoría | Modelado de datos | Gustavo | [@Rhezm](https://github.com/Rhezm) |
| [#3](planificacion/issues/ISSUE_3_etl_mercado_publico.md) | ETL Mercado Público y demostración de falla | Ingeniería ETL, fuente 1 | Jairo | [@JAlbornozPIXX](https://github.com/JAlbornozPIXX) |
| [#4](planificacion/issues/ISSUE_4_etl_banco_central.md) | ETL Banco Central y cruce de monedas | Ingeniería ETL, fuente 2 | Benja | [@Sonnyxpj](https://github.com/Sonnyxpj) |
| [#5](planificacion/issues/ISSUE_5_kpis_informe_presentacion.md) | KPIs, DAG de indicadores, informe y presentación | Análisis y entregables | Sofia | [@Sofia-espinosa](https://github.com/Sofia-espinosa) |


