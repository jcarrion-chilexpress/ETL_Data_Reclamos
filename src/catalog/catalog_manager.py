## src/catalog/catalog_manager.py
from typing import TypedDict
from config.config import get_settings
from config.log_config import logger
from src.utils.utils import read_json_file
from functools import lru_cache
from src.catalog.models import TableConfig


class CatalogManager:
    def __init__(self):
        settings = get_settings()
        ok, tablas = read_json_file(
            settings.config_path,
            "tablas"
        )
        if not ok:
            raise ValueError(
                "No fue posible cargar el catalogo"
            )

        self.tablas:list[TableConfig] = [
            TableConfig(**tabla) 
            for tabla in tablas]

        self._tablas_por_nombre = {
            tabla.nombre: tabla
            for tabla in self.tablas
        }

    def obtener_tabla(self,nombre_tabla: str) -> TableConfig:
        logger.info("Obteniendo datos de %s",nombre_tabla)

        try:
            return self._tablas_por_nombre[nombre_tabla]

        except KeyError:
            raise ValueError(
                f"No existe la tabla '{nombre_tabla}'"
            )

@lru_cache(maxsize=1)
def get_catalogo_manager():
    return CatalogManager()

