# deuda_publica/api_client.py
import logging
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://datos.hacienda.gov.py/odmh-api-v1/rest/api/v1/deudaPublica/deuda"
TIMEOUT_SECONDS = 30

def fetch_page(page):
    params = {"page": page}

    try:
        response = requests.get(BASE_URL, params=params, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as exc:
        logger.error("Error fetching page %s from Deuda Pública API: %s", page, exc)
        raise
