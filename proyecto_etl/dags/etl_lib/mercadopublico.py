"""ETL API Mercado Público (ChileCompra) - Licitaciones.

Documentación: https://www.chilecompra.cl/api/
Requiere ticket (gratuito, uno por persona, 10.000 consultas diarias; se solicita con Clave Única)
"""

import json
import logging
import random
import time
import unicodedata
from datetime import date, datetime, timedelta

import requests
from airflow.models import Variable

from etl_lib.common import (ApiError, Auditoria, asegurar_dim_tiempo, get_json,
                            http_session, pct_nulos, pg_cursor)

log = logging.getLogger(__name__)

DEFAULT_URL = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"

ESTADOS = {5: "Publicada", 6: "Cerrada", 7: "Desierta", 8: "Adjudicada",
           18: "Revocada", 19: "Suspendida"}

ERRORES_REINTENTABLES = {10500, 10501}

#configuracion y acceso a la api

def _cfg():
    #devuelve la url_base, ticket, tope_de_detalles_por_run
    return (Variable.get("mp_base_url", default_var=DEFAULT_URL),
    Variable.get("mp_ticket"),
    int(Variable.get("mp_max_detalle", default_var=150)))


def _consultar(session, url, params, intentos=4):
    #si se excede la cuota de peticiones espera y reintenta
    for i in range(1, intentos + 1):
        status, data = get_json(session, url, params, min_interval=0.5)
        codigo = data.get("Codigo")
        if codigo is None:
            return status, data
        if codigo in ERRORES_REINTENTABLES and i < intentos:
            espera = 5*i
            log.warning("Mercado Publico codigo %s (%s); reintento en %ss",
                        codigo, data.get("Mensaje"), espera)
            time.sleep(espera)
            continue
        raise ApiError(f"Mercado publico codigo= {codigo}: {data.get('Mensaje')}")
        #el bluce siempre deberia retornar o lanzar antes de llegar hasta aqui
    raise ApiError("Mercado publico: reintentos agotados")

    