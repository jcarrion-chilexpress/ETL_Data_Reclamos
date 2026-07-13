CREATE TABLE IF NOT EXISTS {full_name} (
  Mes STRING,
  Clasificacion STRING,
  OT STRING,
  Valor_Declarado_CLP DOUBLE,
  Fecha_Creacion TIMESTAMP,
  Usuario STRING,
  fecha_carga TIMESTAMP)
USING DELTA
{partition_by};
