SELECT
session_id,
id,
message_date,
history,
fecha_inicio,
fecha_fin,
cantidad_mensajes,
mensajes_usuario,
mensajes_bot,
tipo_sesion,
duracion_minutos,
isn
from adl_sandbox.ext_jcarrion.t_emilia_dashboard_base as a
where message_date >= current_date()-{dias}



