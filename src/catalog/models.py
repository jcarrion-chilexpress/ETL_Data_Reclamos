## src/catalog/models.py
from dataclasses import dataclass
from typing import NotRequired

@dataclass(slots=True)
class TableConfig:
    nombre: str
    origen:str
    python_path: str | None
    python_function: str | None
    catalog: str
    schema: str
    query_sql_path: str
    sql_create_path: str
    modo: str
    partition_by: str | None
    where: int
    primary_key: list[str]
    descripcion: str

    @property
    def full_name(self):

        return f"{self.catalog}.{self.schema}.{self.nombre}"

    @property
    def query_name(self):
        return self.query_sql_path.split('/')[1]

