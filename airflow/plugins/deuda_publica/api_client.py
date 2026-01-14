# deuda_publica/api_client.py
import json
import ssl
from urllib.request import urlopen

# URL base de la api
BASE_URL = "https://datos.hacienda.gov.py/odmh-api-v1/rest/api/v1/deudaPublica/deuda"

# obtenemos una página específica de la API y devuelve el JSON como dict
def fetch_page(page: int) -> dict:
    url = f"{BASE_URL}?page={page}"

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    # ejecuta la request HTTP y parsea la respuesta JSON
    with urlopen(url, context=context, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
