## src/flow/flow.py
from src.utils.utils import (crear_directorios,save_csv_file)
from src.catalog.catalog_manager import get_catalogo_manager
from src.catalog.table_manager import TableManager
from src.catalog.dataframe_manager import DataFrameManager
from config.log_config import logger

import pandas as pd


def step_procesar_tabla(spark,table_name):
    ## --------------------------------- ##
    crear_directorios()
    ## --------------------------------- ##
    catalog = get_catalogo_manager()
    df_manager = DataFrameManager(spark)
    ## --------------------------------- ##
    table = catalog.obtener_tabla(
        table_name
        )

    dfs = df_manager.build(table)
    logger.info(f"dfs {table_name} con {dfs.count()} Datos" )

    ## --------------------------------- ##
    TableManager(spark).save(
                            dfs,
                            table)
    ## --------------------------------- ##



