# config/config.py
from pathlib import Path
from functools import lru_cache
from typing import Optional
from pydantic import SecretStr,Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # ==================================================
    # Sesion Spark
    # ==================================================
    ambiente:str = 'local'

    # ==================================================
    # Databricks (para ejecución local vía REST API)
    # ==================================================
    databricks_server_hostname: str = ""
    databricks_token:str = ""
    databricks_http_path: str = ""
    databricks_cluster_id:str = ""
    default_notebook_path: str = ""

    default_sandbox:str = "adl_sandbox.ext_jcarrion"
    # ==================================================
    # Paths
    # ==================================================
    base_path: Path = BASE_DIR
    data_path: Path = BASE_DIR / "data"
    logs_path: Path = BASE_DIR / "logs"
    reclamos_path: Path = BASE_DIR / "data" /"reclamos"
    config_path: Path = BASE_DIR / "config" /"config_sql.json"
    # ==================================================
    # Logging
    # ==================================================
    archivo_log: str = "ETL_Data_Process_Emilia"
    # ==================================================
    # tabla Process
    # ==================================================
    tabla_sentimientos_emilia:str = "sentimientos_emilia_dashboard"
    tabla_dashboard: str = (
        "adl_sandbox.cmontenegroo.emilia_dashboard_base"
    )
    # ==================================================
    # Mongo Config
    # ==================================================
    mongo_uri_default:str ="mongodb://AutomatizacionIA:du4UjPdnoIet91y3@cxp-ia-prod-shard-00-00.mv6l8y.mongodb.net:27017,cxp-ia-prod-shard-00-01.mv6l8y.mongodb.net:27017,cxp-ia-prod-shard-00-02.mv6l8y.mongodb.net:27017/?replicaSet=atlas-zwxmkl-shard-0&ssl=true&authSource=admin&retryWrites=true&w=majority&appName=cxp-ia-prod"
    mongo_db_default:str  ="gr-comercial"
    monog_collection_name_default:str = "usage_logs"

    # ==================================================
    # Pydantic
    # ==================================================

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ==================================================
    # Archivos
    # ==================================================

    @property
    def salida_default(self) -> Path:
        return self.data_path / "sentimientos_pbi.parquet"

    @property
    def datos_default(self) -> Path:
        return self.data_path / "emilia_dashboard_base.parquet"

    # ==================================================
    # Entorno
    # ==================================================

    @property
    def es_databricks(self) -> bool:
        try:
            from pyspark.sql import SparkSession
            return SparkSession.getActiveSession() is not None

        except Exception:
            return False

    @property
    def workspace_url(self) -> Optional[str]:
        if not self.es_databricks:
            return None
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()

            return spark.conf.get(
                "spark.databricks.workspaceUrl",
                None,
            )

        except Exception:
            return None


@lru_cache
def get_settings() -> Settings:
    return Settings()


get_settings()

