from pathlib import Path
import pandas as pd
from config.config import get_settings
from config.log_config import logger

from pyspark.sql.functions import expr

settings = get_settings()

def read_parquet_reclamos(file_name:str) -> pd.DataFrame:
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
    path_file = get_settings().reclamos_path

    ruta = Path(path_file,file_name +'.parquet')
    logger.info(f'Leyendo archivo {ruta}')
    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe el archivo: {ruta}"
        )
    return pd.read_parquet(ruta)
