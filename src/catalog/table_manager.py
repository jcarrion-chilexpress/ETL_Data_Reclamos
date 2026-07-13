from delta.tables import DeltaTable
from config.log_config import logger
from src.catalog.models import TableConfig
from src.catalog.sql_manager import SQLManager

class TableManager:
    def __init__(self, spark):
        self.spark = spark
        self.sql_manager = SQLManager()

    # -------------------------------------------------
    def save(self,df,table: TableConfig):
        modo = table.modo.lower()
        logger.info(
            "Guardando %s en modo %s",
            table.full_name,
            modo,
        )

        if modo == "overwrite":
            return self.overwrite(df,table)

        if modo == "append":
            return self.append(df,table)

        if modo == "merge":
            return self.merge(df,table)

        raise ValueError(
            f"Modo {modo} no soportado.")

    # -------------------------------------------------

    def overwrite(self,df,table: TableConfig):
        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
        )

        if table.partition_by:
            writer = writer.partitionBy(
                table.partition_by
            )

        writer.saveAsTable(
            table.full_name
        )

        logger.info("%s sobrescrita.",table.full_name)

    # -------------------------------------------------

    def append(self,df,table: TableConfig):
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                table.full_name
            )
        )

        logger.info("%s append OK.",table.full_name)

    # -------------------------------------------------    
    def exists(self, table: TableConfig) -> bool:
        table_name = table.full_name

        try:
            if self.spark.catalog.tableExists(table_name):
                logger.info(f'la tabla {table.full_name} EXISTE !')
                return True

            logger.info(f'la tabla {table.full_name} No existe')

            sql = SQLManager.read(table.sql_create_path,
                                full_name = table.full_name
                                ,constraint_pk = str(table.full_name).replace(".","_")
                                ,primary_key = ','.join(table.primary_key)
                                ,partition_by = table.partition_by)

            self.create_table(table_name,sql)
            return True
        except Exception as e:
            return False

    # -------------------------------------------------

    def create_table(self,table_name,sql_create: str):
        logger.info(f"Creando tabla : {table_name}")
        try:
            print('\n',sql_create,'\n')
            # self.spark.sql(sql_create)
            logger.info(f"Tabla : {table_name} creada exitosamente")
        except Exception as e:
            logger.error('Error al creara tabla %s',table_name,e)

    # -------------------------------------------------
    def merge(self,df,table: TableConfig):
        respond = self.exists(table)
        if not respond:
            raise ValueError(f'Error al crear tabla {table.full_name}')

        try:
            delta = DeltaTable.forName(
                self.spark,
                table.full_name)

            condicion = " AND ".join(
                [
                    f"t.{c}=s.{c}"
                    for c in table.primary_key
                ]
            )

            (
                delta.alias("t")
                .merge(
                    df.alias("s"),
                    condicion
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            logger.info("%s merge OK.",table.full_name)
        except Exception as e:
            logger.exception("Error al hacer Merge: %s",str(e))

    # -------------------------------------------------

    def optimize(self,table: TableConfig):
        self.spark.sql(
            f"OPTIMIZE {table.full_name}")

    # -------------------------------------------------

    def vacuum(self,table: TableConfig,horas=168):
        self.spark.sql(
            f"VACUUM {table.full_name} RETAIN {horas} HOURS"
        )

    
