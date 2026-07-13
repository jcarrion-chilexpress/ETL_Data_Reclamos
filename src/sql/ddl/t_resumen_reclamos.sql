CREATE TABLE IF NOT EXISTS {full_name} (
  Mes STRING,
  Clasificacion STRING,
  Cantidad INT,
  Suma_Total_CLP DOUBLE,
  Promedio_CLP DOUBLE,
  fecha_carga TIMESTAMP)
USING DELTA
{partition_by};

