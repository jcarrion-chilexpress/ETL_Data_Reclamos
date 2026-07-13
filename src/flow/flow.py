## src/flow/flow.py
## main.py
import sys
from src.utils.utils import (crear_directorios)
from src.infra.spark import get_spark
from src.catalog.catalog_manager import get_catalogo_manager
from src.catalog.table_manager import TableManager
from src.catalog.dataframe_manager import DataFrameManager


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

    ## --------------------------------- ##
    TableManager(spark).save(
                            dfs,
                            table)
    ## --------------------------------- ##



