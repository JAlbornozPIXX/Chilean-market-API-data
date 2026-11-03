#!/bin/bash
# crea la base de metadatos de airflow (separada de la bd analitica).
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE airflow_meta;"
