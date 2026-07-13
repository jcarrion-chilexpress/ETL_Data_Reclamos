## src/utils/utils.py
import os
import json
from typing import Any
from pathlib import Path
import pandas as pd
from config.config import get_settings
from config.log_config import logger
import unidecode
import re
from pyspark.sql.functions import expr

settings = get_settings()

# -------------------------------------------------- #
def get_fecha_carga():
    return expr("current_timestamp()")

# -------------------------------------------------- #
def clear_terminal():
    os.system('cls')

# -------------------------------------------------- #
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    columnas = []
    for columna in df.columns:
        texto = unidecode.unidecode(columna)
        texto = texto.strip()
        # Reemplazar espacios por "_"
        texto = re.sub(r"\s+", "_", texto)
        # Dejar solo letras, números y "_"
        texto = re.sub(r"[^A-Za-z0-9_]", "", texto)
        columnas.append(texto)
    df.columns = columnas
    return df

# -------------------------------------------------- #
def save_csv_file(df:pd.DataFrame
            ,file_name:str
            ,path_file:Path|None = None
            ) -> tuple[bool,Path]:

    if path_file is None:
        path_file = settings.data_path

    path_csv = Path(path_file, file_name+".csv")
    df_csv = clean_df(df)
    try:
        logger.info(f'Guardando archivo {path_csv}')
        df_csv.to_csv(path_csv, sep=';',header=True)
        return True,path_csv

    except Exception as e:
        logger.exception(f'Error guardando CSV{e}')
        return False,Path("data")

# -------------------------------------------------- #
def save_parquet(df:pd.DataFrame
                 ,file_name:str
                 ,path_file:Path|None = None
                 ,save_csv:bool = False
                 ) -> tuple[bool,Path] :

    df_parquet = clean_df(df)
    
    if path_file is None:
        path_file = settings.data_path

    if save_csv:
        save_csv_file(df_parquet,file_name,path_file)

    path_parquet = Path(path_file, file_name+".parquet")

    try:
        logger.info(f'Guardando archivo {path_parquet}')
        df_parquet.to_parquet(path_parquet)
        return True,path_parquet

    except Exception as e:
        logger.exception(f'Error guardando Parquet{e}')
        return False,Path("data")

# -------------------------------------------------- #
def read_parquet(file_name:str,path_file:Path|None = None) -> pd.DataFrame:
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
    if path_file is None:
        path_file = get_settings().data_path

    ruta = Path(path_file,file_name +'.parquet')
    logger.info(f'Leyendo archivo {ruta}')
    if not ruta.exists():
        raise FileNotFoundError(
            f"No existe el archivo: {ruta}"
        )
    return pd.read_parquet(ruta)

# -------------------------------------------------- #
def crear_directorios() -> tuple[bool,str]:
    try:
        for path in (
            get_settings().data_path,
            get_settings().logs_path,
            get_settings().config_path,
            get_settings().reclamos_path):

            if path.exists():
                continue
            else:
                path.mkdir(parents=True, exist_ok=True)
        logger.info(f'Directorios Base creados exitosamente')
        return True,"Directorios Base creados exitosamente"

    except Exception as e:
        logger.exception(f'Error al crear directorios Base {e}')
        return False,"Error al crear directorios Base {e}"


# -------------------------------------------------- #
def read_sql_file(file_path: str, **kwargs) -> tuple[bool, str]:
    """
    file_path: Path del archivo SQL
    kwargs : recibe el columna_nombre = filtro
    """
    try:
        if not file_path:
            logger.error(
            "No existe query_sql_path en el archivo json")

        if not Path(file_path).is_file():
            logger.error("El archivo %s no existe.", file_path)
            return False, ""

        with open(file_path, "r", encoding="utf-8") as file:
            sql = file.read()

        if kwargs:
            sql = sql.format(**kwargs)

        logger.info("El archivo %s existe.", file_path)
        return True, sql

    except Exception as e:
        logger.exception("Error al leer SQL: %s", e)
        return False, ""

# -------------------------------------------------- #
def read_json_file(
    file_path: Path,
    nombre: str | None = None
    ) -> tuple[bool,dict|Any]:
    try:
        if not Path(file_path).is_file():
            logger.error("El archivo JSON no existe: %s", file_path)
            return False,{}

        logger.info("El archivo JSON existe: %s",file_path)
        with open(file_path, encoding="utf-8") as file:
            catalogo = json.load(file)

        ## Si no se solicita una clave específica,
        ## retorna todo el contenido del JSON.
        if nombre is None:
            return True,catalogo

        catalogo_result = catalogo.get(nombre) 
        if catalogo_result is None:
            logger.warning(f'''No existe: {nombre} dentro del catalogo !''')
            return False,{}
        else:
            logger.info(f'''Catalogo disponible: {nombre} ''')
            return True,catalogo_result

    except Exception as e:
        logger.exception('Error al leer archivo JSON: %s',e)
        return False,{}

# -------------------------------------------------- #
