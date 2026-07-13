CREATE TABLE IF NOT EXISTS {full_name} (
  Mes STRING,
  Clasificacion STRING,
  Suma_Total_CLP DOUBLE,
  Cantidad BIGINT,
  fecha_carga TIMESTAMP)
USING DELTA
{partition_by};
