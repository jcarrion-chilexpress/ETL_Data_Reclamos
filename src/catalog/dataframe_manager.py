import importlib
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
try:
    # Disponible cuando usas Databricks Connect
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
except Exception:
    ConnectDataFrame = ()
from config.log_config import logger
from src.catalog.sql_manager import SQLManager
from src.utils.utils import get_fecha_carga


class DataFrameManager:
    def __init__(self, spark):
        self.spark = spark
    # --------------------------------------------------------
    def build(self, table):
        logger.info(
            "Generando DataFrame desde %s %s",
            table.origen,
            table.nombre,            
        )
        if table.origen == "python":
            df = self._from_python(table)

        elif table.origen == "sql":
            df = self._from_sql(table)

        else:
            raise ValueError(
                f"Origen '{table.origen}' no soportado.")

        df = df.withColumn(
            "fecha_carga",
            get_fecha_carga()
        )
        logger.info(df.printSchema())
        return df

    # --------------------------------------------------------

    def _from_sql(self, table):
        sql = SQLManager.read(
            table.query_sql_path,
            dias=table.where
        )
        return self.spark.sql(sql)

    # --------------------------------------------------------
    def _from_python(self, table):
        logger.info(
            "Ejecutando %s.%s",
            table.python_path,
            table.python_function)

        modulo = importlib.import_module(
            table.python_path)

        funcion = getattr(
            modulo,
            table.python_function)

        resultado = funcion(table.nombre)
        return self._normalize_dataframe(resultado)

    # --------------------------------------------------------
    def _normalize_dataframe(self, resultado):
        """
        Convierte cualquier resultado a Spark DataFrame.
        Soporta:

            - Spark DataFrame
            - Databricks Connect DataFrame
            - Pandas DataFrame
        """
        logger.info(
            "Normalizando DataFrame (%s)",
            type(resultado).__name__
        )

        # ----------------------------------------------------
        # Spark DataFrame (cluster)
        # ----------------------------------------------------
        if isinstance(resultado, SparkDataFrame):
            return resultado

        # ----------------------------------------------------
        # Databricks Connect DataFrame
        # ----------------------------------------------------
        if ConnectDataFrame and isinstance(
            resultado,
            ConnectDataFrame
        ):
            return resultado

        # ----------------------------------------------------
        # Pandas
        # ----------------------------------------------------
        if isinstance(resultado, pd.DataFrame):
            if resultado.empty:
                raise ValueError(
                    "El DataFrame de Pandas está vacío."
                )
            logger.info(
                "Convirtiendo Pandas -> Spark"
            )
            return self.spark.createDataFrame(
                resultado)
        # ----------------------------------------------------
        # None
        # ----------------------------------------------------
        if resultado is None:
            raise ValueError(
                "La función retornó None."
            )
        # ----------------------------------------------------
        raise TypeError(
            f"Tipo no soportado: {type(resultado)}"
        )
    
