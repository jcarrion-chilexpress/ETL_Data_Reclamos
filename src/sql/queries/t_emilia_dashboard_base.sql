WITH mensajes AS (
  SELECT
    session_id,
    balancer_mongo_id,
    role,
    content,
    message_timestamp,
    cast(message_timestamp as date) message_date,
    conversation_expires_at,
    conversation_updated_at,
    sum(CASE WHEN lower(content) RLIKE 'reclamo*|rut*|correo electrónico*|teléfono*
                                        |correo electronico*|telefono*'
        THEN 1 ELSE 0 END) AS pidio_datos,
    sum(CASE
        WHEN lower(content) RLIKE 'responde.*sí|ya existe un reclamo*'
        THEN 1 ELSE 0 END) AS ss_creacion_reclamo,
    MAX(CASE
        WHEN content RLIKE 'CAS-[A-Z0-9-]+'
        THEN 1 ELSE 0 END ) AS genero_caso,
    MAX(regexp_extract(
            content,
            '(CAS-[A-Z0-9-]+)',
            1)) AS id_reclamo,
    sum(CASE 
        WHEN lower(content) RLIKE 'hubo un problema al crear tu reclamo en este momento*|no fue posible crear el reclamo porque*'
        THEN 1 ELSE 0 END) AS error_reclamos

  FROM adl_sandbox.nriosm.conversation_session_history as a
    where cast(message_timestamp as date) >= current_date() - {dias}
    group by all
)
,encuestas AS (
    SELECT
        CAST(IdUsuario AS STRING) AS id,
        Fecha,
        TRY_CAST(EvalIA AS INT) AS eval_ia,
        TRY_CAST(EvalAcceso AS INT) AS eval_acceso,
        EvalResolvReq AS resolucion,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(IdUsuario AS STRING)
            ORDER BY Fecha
        ) AS orden_enc
    FROM adl_gold.enol_v2.t_captura_api_onemarketer_encuesta_data_cruda
    WHERE Fecha >= current_date() - {dias}
      AND AtencionIA = 'Si'
      AND OperadorAbre = 'robot'
      AND OperadorCierra = 'robot'
      AND EvalIA IS NOT NULL
      AND TRIM(EvalIA) <> ''
)
,conversaciones AS (
    SELECT
        session_id,
        cast(message_timestamp as date) as message_date,
        MAX(balancer_mongo_id) AS id,
        MAX(conversation_expires_at) AS expires_at,
        MAX(conversation_updated_at) AS updated_at,
        transform(
            sort_array(
                collect_list(
                    named_struct(
                        'timestamp', message_timestamp,
                        'role', role,
                        'content', content
                    )
                )
            ),
            x -> named_struct(
                'role', x.role,
                'content', x.content,
                'timestamp', CAST(x.timestamp AS STRING)
            )
        ) AS history
    FROM mensajes
    GROUP BY session_id,cast(message_timestamp as date)
 )
,conversaciones_final AS (
    SELECT
        c.*,
        history[0].timestamp AS primer_timestamp,
        element_at(history, -1).timestamp AS ultimo_timestamp,
        to_timestamp(history[0].timestamp) AS fecha_inicio,
        to_timestamp(element_at(history, -1).timestamp) AS fecha_fin,
        size(history) AS cantidad_mensajes,
        size(filter(history, x -> x.role = 'user')) AS mensajes_usuario,
        size(filter(history, x -> x.role = 'assistant')) AS mensajes_bot,
        ROUND(size(history) / 2.0, 1) AS turnos_conversacion,
        filter(history, x -> x.role = 'user') as si,
        CASE
            WHEN size(filter(history, x -> x.role = 'user')) = 1
                THEN 'Abandono'
            ELSE 'Conversacion'
        END AS tipo_sesion,
        ROUND(
            (
                unix_timestamp(to_timestamp(element_at(history, -1).timestamp))
                - unix_timestamp(to_timestamp(history[0].timestamp))
            ) / 60.0,
            2
        ) AS duracion_minutos,
        unix_timestamp(to_timestamp(element_at(history, -1).timestamp))
        - unix_timestamp(to_timestamp(history[0].timestamp))
        AS duracion_segundos,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(id AS STRING)
            ORDER BY to_timestamp(element_at(history, -1).timestamp)
        ) AS orden_conv
    FROM conversaciones c
),
ISN as (SELECT
    cast(Fecha AS STRING) AS Fecha,
    ROUND(
        (
            SUM(CASE WHEN eval_ia IN (6,7) THEN 1 ELSE 0 END)
            -
            SUM(CASE WHEN eval_ia BETWEEN 1 AND 4 THEN 1 ELSE 0 END)
        ) * 100.0 / COUNT(*),
        2
    ) AS isn
FROM encuestas
WHERE eval_ia BETWEEN 1 AND 7
GROUP BY cast(Fecha AS STRING)
)

,reclamos as
(
    SELECT
        a.message_date,
        a.session_id,
        MAX(a.id_reclamo) AS id_reclamo,
        SUM(a.pidio_datos) AS pidio_datos,
        SUM(a.ss_creacion_reclamo) AS solicitud_creacion_reclamo,
        MAX(a.genero_caso) AS genero_caso,
        SUM(a.error_reclamos) AS error_reclamos,
        CASE
            WHEN MAX(genero_caso) = 1
                THEN 'RECLAMO_CREADO'
            WHEN SUM(ss_creacion_reclamo) > 0
                AND MAX(genero_caso) = 0
                THEN 'RECLAMO_INCONCLUSO'
            WHEN SUM(pidio_datos) > 0
                AND MAX(genero_caso) = 0
                THEN 'RECLAMO_EN_PROCESO'
            ELSE 'SIN_RECLAMO'
        END AS estado_reclamo
            FROM mensajes a
            GROUP BY a.message_date,a.session_id
)

SELECT
    c.session_id,
    c.id,
    c.message_date,
    to_timestamp(c.expires_at) AS expires_at,
    to_timestamp(c.updated_at) AS updated_at,
    -----------------------
    c.history,
    -----------------------
    c.primer_timestamp,
    c.ultimo_timestamp,
    c.fecha_inicio,
    c.fecha_fin,
    c.cantidad_mensajes,
    c.mensajes_usuario,
    c.mensajes_bot,
    c.turnos_conversacion,
    c.tipo_sesion,
    c.duracion_minutos,
    c.duracion_segundos,
    ------ Orden FIFO utilizado para el matching
    c.orden_conv,
    e.orden_enc,

    ------ Datos de encuesta
    e.Fecha AS fecha_encuesta,
    e.eval_ia,
    e.eval_acceso,
    e.resolucion,
    ------ Datos de Reclamos
    reclamos.* EXCEPT(message_date,session_id),
    isn.isn

FROM conversaciones_final as c
LEFT JOIN ISN as isn
    ON c.message_date = isn.fecha
LEFT JOIN encuestas as e
    ON CAST(c.id AS STRING) = e.id
   AND c.orden_conv = e.orden_enc
LEFT JOIN reclamos as reclamos
on c.session_id = reclamos.session_id
and c.message_date = reclamos.message_date
