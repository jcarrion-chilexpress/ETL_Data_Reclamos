### src/extract/databricks_client.py
import base64
import io
import json
import os
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import requests
import pandas as pd
from config.config import get_settings
from config.log_config import logger

# Ruta en Workspace (según ejecuciones previas del notebook en Databricks)
DEFAULT_NOTEBOOK_PATH = get_settings().default_notebook_path
TABLA_DASHBOARD = get_settings().tabla_dashboard

def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def cluster_id_from_http_path(http_path: str) -> str:
    path = http_path.strip()
    if path and not path.startswith("/"):
        path = "/" + path
    parts = path.rstrip("/").split("/")
    if not parts[-1]:
        raise ValueError("DATABRICKS_HTTP_PATH inválido")
    return parts[-1]


class DatabricksRestClient:
    """Cliente REST: mismas credenciales .env (host, token, HTTP_PATH → cluster)."""

    def __init__(self):
        self.host = _env("DATABRICKS_SERVER_HOSTNAME")
        self.token = _env("DATABRICKS_TOKEN")
        self.http_path = _env("DATABRICKS_HTTP_PATH")
        if not self.host or not self.token or not self.http_path:
            raise ValueError(
                "Faltan DATABRICKS_SERVER_HOSTNAME, DATABRICKS_TOKEN o DATABRICKS_HTTP_PATH en .env"
            )
        if not self.http_path.startswith("/"):
            self.http_path = "/" + self.http_path
        self.cluster_id = cluster_id_from_http_path(self.http_path)
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.base_url = f"https://{self.host}/api/1.2"
        self.api_21 = f"https://{self.host}/api/2.1"
        self.context_id = None

    def create_context(self):
        url = f"{self.base_url}/contexts/create"
        payload = {"language": "python", "clusterId": self.cluster_id}
        logger.info("Creando contexto en cluster %s...", self.cluster_id)
        resp = requests.post(url, headers=self.headers, json=payload, timeout=30)
        if resp.status_code != 200:
            logger.error(
                "Error creando contexto. Status: %s - Response: %s",
                resp.status_code,
                resp.text,
            )
            raise Exception(f"Failed to create context: {resp.text}")

        self.context_id = resp.json()["id"]
        logger.info("Contexto creado exitosamente: %s", self.context_id)
        self._wait_for_context()

    def _wait_for_context(self):
        url = f"{self.base_url}/contexts/status"
        params = {"clusterId": self.cluster_id, "contextId": self.context_id}
        for _ in range(120):
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            status = resp.json().get("status")
            if status == "Running":
                logger.info("Contexto %s listo para usar", self.context_id)
                return
            if status == "Error":
                logger.error("Error durante la creación del contexto %s", self.context_id)
                raise Exception("Context creation failed.")
            time.sleep(1)
        raise Exception("Timeout waiting for context to be ready")

    def execute_command(self, code: str, language: str = "python"):
        if not self.context_id:
            self.create_context()
        url = f"{self.base_url}/commands/execute"
        payload = {
            "language": language,
            "clusterId": self.cluster_id,
            "contextId": self.context_id,
            "command": code,
        }
        resp = requests.post(url, headers=self.headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise Exception(f"Execute failed: {resp.text}")
        command_id = resp.json()["id"]
        return self._wait_for_command(command_id)

    def execute_python(self, code):
        return self.execute_command(code, language="python")

    def execute_sql(self, sql: str) -> dict:
        sql = sql.strip()
        if sql.lower().startswith("%sql"):
            sql = sql[4:].strip()
        return self.execute_command(sql, language="sql")

    def _wait_for_command(self, command_id) -> dict:
        url = f"{self.base_url}/commands/status"
        params = {
            "clusterId": self.cluster_id,
            "contextId": self.context_id,
            "commandId": command_id,
        }
        while True:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            data = resp.json()
            status = data.get("status")
            if status == "Finished":
                results = data.get("results", {})
                if results.get("resultType") == "error":
                    raise Exception(f"Execution Error: {results.get('cause')}")
                return results
            if status in ("Cancelled", "Error"):
                raise Exception(f"Command failed with status: {status}")
            time.sleep(1)

    def execute_sql_as_rows(self, sql: str, limit: int | None = None) -> list[dict]:
        """Ejecuta SQL y devuelve filas como list[dict] (vía spark en el cluster)."""
        body = sql.strip()
        if body.lower().startswith("%sql"):
            body = body[4:].strip()
        if limit is not None:
            body = f"SELECT * FROM ({body}) AS _q LIMIT {int(limit)}"
        return self.fetch_data(body)

    def close(self):
        if self.context_id:
            url = f"{self.base_url}/contexts/destroy"
            payload = {"clusterId": self.cluster_id, "contextId": self.context_id}
            try:
                requests.post(url, headers=self.headers, json=payload, timeout=5)
            except Exception:
                pass
            self.context_id = None

    def _result_text(self, results) -> str:
        if isinstance(results, dict):
            return (results.get("data") or "").strip()
        return (results or "").strip()

    def _fetch_data_raw(self, sql_query: str) -> list[dict]:
        """Consultas pequeñas (pocas filas). Para datasets grandes usar fetch_data_parquet_df."""
        sql_lit = repr(sql_query.strip())
        python_code = f"""
            import json, base64
            from decimal import Decimal
            from datetime import date, datetime
            def default_converter(o):
                if isinstance(o, (datetime, date)): return o.isoformat()
                if isinstance(o, Decimal): return float(o)
                return str(o)
            df = spark.sql({sql_lit})
            rows = [row.asDict() for row in df.collect()]
            payload = json.dumps(rows, default=default_converter, ensure_ascii=True)
            print(base64.b64encode(payload.encode("utf-8")).decode("ascii"))
            """
        results = self.execute_python(python_code)
        text = self._result_text(results)
        if not text:
            return []
        # Quitar saltos de línea por si la API parte el payload
        text = "".join(text.split())
        pad = (-len(text)) % 4
        if pad:
            text += "=" * pad
        raw = base64.b64decode(text.encode("ascii"))
        return json.loads(raw.decode("utf-8"))

    def fetch_data(self, sql_query: str) -> list[dict]:
        return self._fetch_data_raw(sql_query)

    def _dbfs_list(self, api_path: str) -> list[dict]:
        logger.info("Listando archivos DBFS: %s", api_path)
        url = f"https://{self.host}/api/2.0/dbfs/list"
        resp = requests.get(
            url, headers=self.headers, params={"path": api_path}, timeout=60
        )
        if resp.status_code != 200:
            logger.error(
                    "Error listando DBFS %s. Response: %s",
                    api_path,
                    resp.text)
            raise Exception(f"dbfs/list failed: {resp.text}")
        return resp.json().get("files", [])

    def _dbfs_read_all(self, api_path: str) -> bytes:
        url = f"https://{self.host}/api/2.0/dbfs/read"
        offset = 0
        chunks: list[bytes] = []
        while True:
            resp = requests.get(
                url,
                headers=self.headers,
                params={"path": api_path, "offset": offset, "length": 1_048_576},
                timeout=120,
            )
            if resp.status_code != 200:
                raise Exception(f"dbfs/read failed: {resp.text}")
            data = resp.json()
            n = int(data.get("bytes_read") or 0)
            if n == 0:
                break
            chunks.append(base64.b64decode(data["data"]))
            offset += n
            if n < 1_048_576:
                break
        return b"".join(chunks)

    def _dbfs_delete(self, api_path: str) -> None:
        url = f"https://{self.host}/api/2.0/dbfs/delete"
        requests.post(
            url,
            headers=self.headers,
            json={"path": api_path, "recursive": True},
            timeout=60,
        )

    def fetch_data_parquet_df(self, sql_query: str):
        """
        Exporta el SQL a Parquet en DBFS y lo descarga (sin límite ~50KB de stdout).
        """
        import pandas as pd

        export_id = uuid.uuid4().hex
        dbfs_path = f"dbfs:/FileStore/emilia_local_export/{export_id}"
        api_path = f"/FileStore/emilia_local_export/{export_id}"
        sql_lit = repr(sql_query.strip())

        logger.info("Exportando consulta a Parquet (%s)...", dbfs_path)
        code = f"""
        spark.sql({sql_lit}).coalesce(1).write.mode("overwrite").parquet("{dbfs_path}")
        print("PARQUET_OK")
        """
        self.execute_python(code)

        files = self._dbfs_list(api_path)
        part_files = [
            f
            for f in files
            if not f.get("is_dir")
            and ("part-" in f.get("name", "") or str(f.get("path", "")).endswith(".parquet"))
        ]
        if not part_files:
            raise Exception(f"No se encontró parquet en {api_path}: {files}")

        frames = []
        for f in part_files:
            path = f["path"]
            logger.info("Descargando %s ...", path)
            frames.append(pd.read_parquet(io.BytesIO(self._dbfs_read_all(path))))

        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        logger.info("Descarga Parquet: %s filas", len(df))

        try:
            self._dbfs_delete(api_path)
        except Exception:
            logger.warning("No se pudo eliminar export temporal %s", api_path)

        return df

    def import_notebook(self, local_ipynb: Path, workspace_path: str, *, overwrite: bool = True):
        """Sube el .ipynb local al Workspace antes de ejecutarlo."""
        url = f"https://{self.host}/api/2.0/workspace/import"
        payload = {
            "path": workspace_path,
            "format": "JUPYTER",
            "content": base64.b64encode(local_ipynb.read_bytes()).decode("ascii"),
            "overwrite": overwrite,
        }
        logger.info("Importando %s → %s", local_ipynb.name, workspace_path)
        resp = requests.post(url, headers=self.headers, json=payload, timeout=300)
        if resp.status_code != 200:
            raise Exception(f"Import failed: {resp.status_code} {resp.text}")

    def submit_notebook(self, notebook_path: str, run_name: str = "Conversaciones Emilia AKS") -> int:
        """Lanza el notebook en el cluster (Jobs API 2.1)."""
        url = f"{self.api_21}/jobs/runs/submit"
        payload = {
            "run_name": run_name,
            "tasks": [
                {
                    "task_key": "emilia_aks",
                    "notebook_task": {"notebook_path": notebook_path},
                    "existing_cluster_id": self.cluster_id,
                }
            ],
        }
        logger.info("Lanzando notebook %s en cluster %s", notebook_path, self.cluster_id)
        resp = requests.post(url, headers=self.headers, json=payload, timeout=60)
        if resp.status_code != 200:
            raise Exception(f"Submit failed: {resp.status_code} {resp.text}")
        run_id = resp.json()["run_id"]
        logger.info("Run id: %s", run_id)
        return run_id

    def get_run(self, run_id: int) -> dict:
        url = f"{self.api_21}/jobs/runs/get"
        resp = requests.get(
            url, headers=self.headers, params={"run_id": run_id}, timeout=30
        )
        if resp.status_code != 200:
            raise Exception(f"get_run failed: {resp.text}")
        return resp.json()

    def wait_for_run(
        self, run_id: int, *, timeout_sec: int = 3600, poll_sec: int = 15
    ) -> dict:
        deadline = time.time() + timeout_sec
        terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        while time.time() < deadline:
            info = self.get_run(run_id)
            state = info.get("state", {})
            life = state.get("life_cycle_state")
            result = state.get("result_state")
            msg = state.get("state_message", "")
            logger.info("Run %s: %s%s", run_id, life, f" ({result})" if result else "")
            if life in terminal:
                if result == "SUCCESS":
                    return info
                raise Exception(
                    f"Notebook falló: {result}. {msg}".strip()
                )
            time.sleep(poll_sec)
        raise Exception(f"Timeout ({timeout_sec}s) esperando run {run_id}")

    @property
    def run_url_template(self) -> str:
        return f"https://{self.host}/#job/{{run_id}}/run/1"

    def ejecutar_spark_sql(self, sql: str) -> None:
        """DDL/DML vía spark.sql en el cluster (p. ej. CREATE TABLE)."""
        if '"""' in sql:
            raise ValueError("SQL no puede contener triple comilla")
        code = f'spark.sql("""{sql}""")\nprint("spark.sql OK")'
        self.execute_python(code)

    def query_df(self, sql: str, *, via_parquet: bool = False):
        """Filas como pandas.DataFrame."""
        import pandas as pd

        if via_parquet:
            return self.fetch_data_parquet_df(sql)
        return pd.DataFrame(self.fetch_data(sql))

from pyspark.sql import SparkSession

def configurado() -> bool:
    """
    Determina si existe una forma válida de acceder a Databricks.
    """
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return True
    settings = get_settings()
    return all(
        [
            bool(settings.databricks_server_hostname),
            settings.databricks_token is not None,
            bool(settings.databricks_http_path),
        ]
    )


def cargar_dashboard_base(
                    query,
                    client: DatabricksRestClient | None = None) -> pd.DataFrame:
    """
    Carga la tabla base utilizada por el proceso de sentimientos.
    Prioridad:
        1. Si existe una SparkSession activa -> usa spark.sql()
        2. Si no existe -> usa DatabricksRestClient
    Parameters
    ----------
    query  : sql spark 
    client : DatabricksRestClient | None
        Cliente reutilizable opcional.

    Returns
    -------
    pd.DataFrame
    """
    sql = query
    spark = SparkSession.getActiveSession()
    try:
        # ==========================
        # Databricks Notebook
        # ==========================
        if spark is not None:
            return spark.sql(sql).toPandas()
        # ==========================
        # Ejecución Local
        # ==========================
        own_client = client is None

        if own_client:
            client = DatabricksRestClient()

        try:
            return client.query_df(
                sql,
                via_parquet=True
            )
        finally:
            if own_client:
                client.close()

    except Exception as exc:
        msg = str(exc).lower()

        if (
            "table_or_view_not_found" in msg
            or "table not found" in msg
            or "view not found" in msg
        ):
            raise RuntimeError(
                f"No existe la tabla '{TABLA_DASHBOARD}'."
            ) from exc

        raise

def obtener_datos_databricks():
    client = DatabricksRestClient()
    try:
        query = "SELECT * FROM adl_gold.dwh_lyd.carrier_data LIMIT 10"
        return client.fetch_data(query)
    finally:
        client.close()

