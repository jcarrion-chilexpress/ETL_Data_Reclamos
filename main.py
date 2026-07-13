## main.py
import sys
from src.utils.utils import (clear_terminal,read_json_file)
from src.infra.spark import get_spark
from src.flow.flow import step_procesar_tabla
from src.transform.consolidado_reclamos import crear_resumen_reclamos
from src.utils.utils import read_parquet
from config.config import Settings

settings = Settings()
clear_terminal()

def main():
    spark = get_spark()

    crear_resumen_reclamos()
    tablas = ["t_top_clasificaciones_reclamos","t_detalle_reclamos"
                ,"t_evolucion_reclamos","t_resumen_reclamos"]
    ## --------------------------------- ##
    for table in tablas:
        step_procesar_tabla(spark
                            ,table_name=table)
    
    ## --------------------------------- ##

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(40)

