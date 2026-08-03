from pathlib import Path
import pandas as pd
from config.config import get_settings
from config.log_config import logger
from src.transform.consolidado_reclamos import crear_resumen_reclamos
from pyspark.sql.functions import expr

settings = get_settings()

def read_reclamos(file_name:str):
    """
    Lee un archivo parquet y devuelve un DataFrame.
    Parameters
    ----------
    file : str | Path
        Ruta/Nombre del archivo parquet.
    Returns
    -------
    pd.DataFrame
    """
    logger.info(f'Procesando Archivo {file_name}')
    msn,df = crear_resumen_reclamos()

    if msn:
        return df

    if not msn:
        logger.error(f'Error Generando DF {msn}, {df}')
        raise ValueError('Error Generando DF')

