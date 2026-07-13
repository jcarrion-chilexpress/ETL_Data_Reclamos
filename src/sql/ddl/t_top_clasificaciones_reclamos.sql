CREATE TABLE IF NOT EXISTS {full_name} (
  Clasificacion STRING,
  Suma_Total_CLP double,
  Cantidad BIGINT,
  Promedio_CLP double,
  fecha_carga TIMESTAMP)
USING DELTA
{partition_by};
