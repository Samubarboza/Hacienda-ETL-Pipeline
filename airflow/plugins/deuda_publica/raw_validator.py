import json
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential


DATASET_NAME = "deuda_publica"
TOTAL_PAGES = 166  # pAginas reales de la API

class RawDeudaPublicaValidator:
    def __init__(self, storage_account_url: str, file_system_name: str, credential, raw_base_path: str = "deuda_publica", ):
        # base path del dataset RAW
        self.raw_base_path = raw_base_path

        # credencial Azure (Service Principal)
        self.credential = ClientSecretCredential(
            tenant_id=credential["tenant_id"],
            client_id=credential["client_id"],
            client_secret=credential["client_secret"],
        )

        # cliente del Data Lake
        self.service_client = DataLakeServiceClient(account_url=storage_account_url, credential=self.credential,)

        # filesystem (container)
        self.fs_client = self.service_client.get_file_system_client(file_system=file_system_name)


    def _get_latest_execution_date(self) -> str:
        paths = self.fs_client.get_paths(path=self.raw_base_path)

        execution_dates = []

        for p in paths:
            if p.is_directory and "execution_date=" in p.name:
                execution_dates.append(p.name.split("execution_date=")[-1])

        if not execution_dates:
            raise AirflowException("No se encontraron particiones RAW para deuda_publica")

        return max(execution_dates)


    # lista todos los archivos raw, excluye carpetas
    def _list_raw_files(self) -> list:
        paths = self.fs_client.get_paths(path=self.raw_path)
        files = [p for p in paths if not p.is_directory]
        return files

    # extrae el numero de pagina desde el nombre del archivo
    def _extract_page_number(self, file_path: str) -> int:
        # Ejemplo real: page=001.json
        name = file_path.split("/")[-1]
        page_str = name.replace("page=", "").replace(".json", "")
        return int(page_str)


    # valida integridad completa del raw o falla el dag
    def validate_or_fail(self) -> None:
        started = datetime.now(timezone.utc)
        execution_date = self._get_latest_execution_date()
        self.raw_path = f"{self.raw_base_path}/execution_date={execution_date}"

        # obtenemos la lista de archivos raw
        files = self._list_raw_files()

        if not files:
            raise AirflowException(f"No se encontraron archivos RAW en {self.raw_path}")

        pages_loaded = set()
        empty_files = []

        # recorremos cada archivo raw 
        for file in files:
            file_client = self.fs_client.get_file_client(file.name)
            content = file_client.download_file().readall()

            if not content:
                empty_files.append(file.name)
                continue
            # validamos que el contenido sea JSON válido
            try:
                json.loads(content)
            except json.JSONDecodeError:
                raise AirflowException(f"Archivo JSON inválido: {file.name}")

            pages_loaded.add(self._extract_page_number(file.name))

        if empty_files:
            raise AirflowException(f"Archivos vacíos detectados: {empty_files}")

    # calculamos páginas esperadas vs cargadas
        expected_pages = set(range(1, TOTAL_PAGES + 1))
        missing_pages = sorted(expected_pages - pages_loaded)

        if missing_pages:
            raise AirflowException(f"RAW incompleto. Páginas faltantes: {missing_pages}")

        ended = datetime.now(timezone.utc)

        # Si llegó hasta acá - VALIDACION OK
        return
