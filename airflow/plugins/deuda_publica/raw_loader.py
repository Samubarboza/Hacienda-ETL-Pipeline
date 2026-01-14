import json
import logging
import hashlib
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from deuda_publica.api_client import fetch_page

# logger para registrar eventos del proceso en Airflow
logger = logging.getLogger(__name__)

MAX_PAGES = 167

# Clase responsable de cargar datos crudos de la API a la tabla RAW
class RawDeudaPublicaLoader:

    # inicializa fecha de ejecución, conexión a SQL Server y cliente de la API
    def __init__(self, execution_date):
        self.execution_date = execution_date
        self.hook = MsSqlHook(mssql_conn_id="sqlserver_hacienda")

    # recorre todas las páginas de la API y carga cada una en la tabla RAW
    def load_all(self):
        for page in range(1, MAX_PAGES + 1):
            data = fetch_page(page)
            results = data.get("results", [])

            if not results:
                logger.info(f"Página {page} sin resultados. Corte.")
                break

            # inserta el JSON completo en la tabla RAW
            self._insert(page, data)
            logger.info(f"Página {page} cargada ({len(results)} registros)")

    # insertamos el payload JSON con metadatos e idempotencia
    def _insert(self, page: int, data):
        # Genera un hash único por página y fecha de ejecución
        raw = f"deuda_publica|page={page}|execution_date={self.execution_date}"
        request_hash = hashlib.sha256(raw.encode()).hexdigest()

        sql = """
        INSERT INTO raw.raw_deuda_publica
            (payload, source_system, execution_date, page_number, request_hash)
        VALUES
            (%s, %s, %s, %s, %s)
        """
    # ejecutamos el INSERT contra SQL Server
        self.hook.run(sql, parameters=(json.dumps(data, ensure_ascii=False), "odmh_hacienda_py", self.execution_date, page, request_hash), autocommit=True)