## src/infra/spark.py
from functools import lru_cache
from config.config import get_settings
from config.log_config import logger

settings = get_settings()

@lru_cache
def get_spark(ambiente:str = get_settings().ambiente):
    logger.info(f'Creando Sparksession en Env {ambiente}')
    spark = ''

    try:
        if ambiente.lower() == "local":
            from databricks.connect import DatabricksSession
            spark = (
                DatabricksSession.builder
                .remote(
                    host=settings.databricks_server_hostname,
                    token=settings.databricks_token,
                    cluster_id=settings.databricks_cluster_id
                )
                .getOrCreate()
            )
            logger.info(f'Sparksession {ambiente}, Creada Existosamente !')
            return spark

        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            logger.info(f'Sparksession {ambiente}, Creada Existosamente !')

            return spark

    except Exception as e:
        logger.exception(f'Sparksession {ambiente}, con Error:{e}')
        raise RuntimeError(f'No fue posible crear la SparkSession: {e}')


# current_user = (
#     spark.sql("SELECT current_user()")
#         .first()[0]
# )

# logger.info(f"Conectado a Databricks como: {current_user}")

