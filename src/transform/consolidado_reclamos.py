#!/usr/bin/env python3
"""Script que agrupa valor_declarado de usage_logs por mes y todas las clasificaciones encontradas.
VERSIÓN: Solo cuenta el primer registro clasificado por OT (evita duplicados cuando una pieza cambia de clasificación)."""
from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List
from collections import defaultdict
from pymongo import MongoClient

from src.infra.mongo_client import get_mongo_client
from config.log_config import logger
from config.config import get_settings
from src.utils.utils import save_parquet

### Configuración
settings = get_settings()
# MONGO_URI = "mongodb://AutomatizacionIA:du4UjPdnoIet91y3@cxp-ia-prod-shard-00-00.mv6l8y.mongodb.net:27017,cxp-ia-prod-shard-00-01.mv6l8y.mongodb.net:27017,cxp-ia-prod-shard-00-02.mv6l8y.mongodb.net:27017/?replicaSet=atlas-zwxmkl-shard-0&ssl=true&authSource=admin&retryWrites=true&w=majority&appName=cxp-ia-prod"
# DATABASE_NAME = "gr-comercial"
# COLLECTION_NAME = "usage_logs"
FECHA_FIELD = "created_at"
VALOR_FIELD = "valor_declarado"

### Usuarios a excluir (agregar los usuarios que se deseen excluir)
### Para agregar usuarios, modifica la lista: USUARIOS_EXCLUIDOS = ["usuario1", "usuario2", "test_user"]
USUARIOS_EXCLUIDOS: List[str] = [
    "nriosm@chilexpress.cl",
    "cmella@chilexpress.cl",
    "lduarte@chilexpress.cl",
    "nico@sss.cl",
    "vgallardoe@chilexpress.cl",
    "string",
    "usuario_anonimo"
]

#### Clasificaciones a excluir
CLASIFICACIONES_EXCLUIDAS: List[str] = ["n/a", "N/A", "N/a", "n/A"]

##### define los decimales de def generar_dataframes
decimales = 3

############################################
def obtener_datos_por_mes(collection) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Obtiene datos agrupados por mes y todas las clasificaciones encontradas.
    IMPORTANTE: Solo toma el primer registro clasificado por OT (ordenado por fecha de creación).
    Retorna: {mes: {clasificacion: [{'ot': ..., 'valor_declarado': ..., 'fecha_creacion': ..., 'usuario': ...}, ...]}}
    """
    logger.info(f'Obteniendo Datos por Mes')
    pipeline = []
    # Determinar la fecha del documento: usar created_at si existe, si no usar fecha_escaneo
    # Esto soporta tanto el formato antiguo (created_at como string ISO) como el nuevo (fecha_escaneo como Date)
    pipeline.append({
        "$addFields": {
            "created_at_date": {
                "$switch": {
                    "branches": [
                        # Caso 1: created_at existe y es tipo date
                        {
                            "case": {"$eq": [{"$type": f"${FECHA_FIELD}"}, "date"]},
                            "then": f"${FECHA_FIELD}"
                        },
                        # Caso 2: created_at existe y es string → convertir
                        {
                            "case": {"$eq": [{"$type": f"${FECHA_FIELD}"}, "string"]},
                            "then": {
                                "$dateFromString": {
                                    "dateString": f"${FECHA_FIELD}",
                                    "onError": None,
                                    "onNull": None
                                }
                            }
                        },
                        # Caso 3: fecha_escaneo existe y es tipo date (formato nuevo)
                        {
                            "case": {"$eq": [{"$type": "$fecha_escaneo"}, "date"]},
                            "then": "$fecha_escaneo"
                        },
                        # Caso 4: fecha_escaneo existe y es string → convertir
                        {
                            "case": {"$eq": [{"$type": "$fecha_escaneo"}, "string"]},
                            "then": {
                                "$dateFromString": {
                                    "dateString": "$fecha_escaneo",
                                    "onError": None,
                                    "onNull": None
                                }
                            }
                        }
                    ],
                    "default": None
                }
            }
        }
    })
    
    # Filtrar documentos con fecha válida
    pipeline.append({"$match": {"created_at_date": {"$ne": None}}})
    
    # Filtrar solo los que tienen OT válido
    pipeline.append({
        "$match": {
            "ot": {"$ne": "", "$exists": True}
        }
    })
    
    # Filtrar usuarios excluidos ANTES del $project
    # Verificar en todos los campos posibles donde puede estar el usuario
    # Excluir documentos donde CUALQUIERA de los campos contenga un usuario excluido
    if USUARIOS_EXCLUIDOS:
        pipeline.append({
            "$match": {
                "$nor": [
                    {"user": {"$in": USUARIOS_EXCLUIDOS}},
                    {"usuario": {"$in": USUARIOS_EXCLUIDOS}},
                    {"username": {"$in": USUARIOS_EXCLUIDOS}},
                    {"user_id": {"$in": USUARIOS_EXCLUIDOS}}
                ]
            }
        })
    
    # Extraer año y mes, y preparar clasificación
    pipeline.append({
        "$addFields": {
            "año": {"$year": "$created_at_date"},
            "mes": {"$month": "$created_at_date"},
            "clasificacion": {"$ifNull": ["$clasificacion", "Sin clasificación"]},
            "valor_num": {
                "$cond": {
                    "if": {"$and": [{"$ne": [f"${VALOR_FIELD}", None]}, {"$ne": [f"${VALOR_FIELD}", ""]}]},
                    "then": {"$convert": {"input": f"${VALOR_FIELD}", "to": "double", "onError": 0, "onNull": 0}},
                    "else": 0
                }
            },
            "usuario": {
                "$ifNull": [
                    "$user",
                    {
                        "$ifNull": [
                            "$usuario",
                            {
                                "$ifNull": [
                                    "$username",
                                    {
                                        "$ifNull": ["$user_id", "Sin usuario"]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    })
    
    # Filtrar clasificaciones excluidas (n/a, N/A, etc.)
    if CLASIFICACIONES_EXCLUIDAS:
        pipeline.append({
            "$match": {
                "clasificacion": {"$nin": CLASIFICACIONES_EXCLUIDAS}
            }
        })
    
    # Ordenar por OT y fecha de creación (más antiguo primero)
    # Esto asegura que tomemos el primer registro clasificado por OT
    pipeline.append({
        "$sort": {
            "ot": 1,
            "created_at_date": 1
        }
    })
    
    # Agrupar por OT y tomar solo el primer documento (el más antiguo con clasificación válida)
    pipeline.append({
        "$group": {
            "_id": "$ot",
            "primer_registro": {"$first": "$$ROOT"}
        }
    })
    
    # Reemplazar la raíz con el primer registro
    pipeline.append({
        "$replaceRoot": {"newRoot": "$primer_registro"}
    })
    
    # Proyectar campos necesarios
    pipeline.append({
        "$project": {
            "_id": 0,
            "año": 1,
            "mes": 1,
            "clasificacion": "$clasificacion",
            "ot": {"$ifNull": ["$ot", ""]},
            "valor_declarado": "$valor_num",
            "created_at_date": 1,
            "usuario": 1
        }
    })
    
    # Ordenar por año, mes y clasificación para el resultado final
    pipeline.append({
        "$sort": {
            "año": 1,
            "mes": 1,
            "clasificacion": 1
        }
    })
    
    result = list(collection.aggregate(pipeline))
    
    # Agrupar por mes y clasificación
    datos_por_mes: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    
    for doc in result:
        año = doc.get('año', 0)
        mes = doc.get('mes', 0)
        clasificacion = doc.get('clasificacion', 'sin clasificación')
        ot = doc.get('ot', '')
        valor = doc.get('valor_declarado', 0)
        fecha_creacion = doc.get('created_at_date')
        usuario = doc.get('usuario', 'Sin usuario')
        mes_key = f"{año}-{mes:02d}"
        
        if ot:
            datos_por_mes[mes_key][clasificacion].append({
                'ot': ot,
                'valor_declarado': valor,
                'fecha_creacion': fecha_creacion,
                'usuario': usuario
            })
    
    return dict(datos_por_mes)

############################################
def generar_dataframes(datos_por_mes: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> Dict[str, pd.DataFrame]:
    """Genera DataFrames de pandas con datos agrupados por mes y clasificación."""
    # 1. DataFrame de Resumen General
    logger.info(f'Generando Dataframes')
    resumen_data = []
    for mes_key in sorted(datos_por_mes.keys()):
        datos_mes = datos_por_mes[mes_key]
        for clasificacion in sorted(datos_mes.keys()):
            ots = datos_mes[clasificacion]
            cantidad = len(ots)
            suma = sum(round(ot.get('valor_declarado', 0),decimales) for ot in ots)
            promedio = suma / cantidad if cantidad > 0 else 0
            resumen_data.append({
                'Mes': mes_key,
                'Clasificación': clasificacion,
                'Cantidad': cantidad,
                'Suma Total (CLP)': round(suma,decimales),
                'Promedio (CLP)': round(promedio,decimales)
            })
    
    df_resumen = pd.DataFrame(resumen_data)
    
    # 2. DataFrame de Detalle Completo
    detalle_data = []
    for mes_key in sorted(datos_por_mes.keys()):
        datos_mes = datos_por_mes[mes_key]
        for clasificacion in sorted(datos_mes.keys()):
            for ot_data in datos_mes[clasificacion]:
                fecha_creacion = ot_data.get('fecha_creacion')
                if fecha_creacion and isinstance(fecha_creacion, datetime):
                    fecha_str = fecha_creacion.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fecha_str = str(fecha_creacion) if fecha_creacion else 'N/A'                
                detalle_data.append({
                    'Mes': mes_key,
                    'Clasificación': clasificacion,
                    'OT': ot_data.get('ot', ''),
                    'Valor Declarado (CLP)': round(ot_data.get('valor_declarado', 0),decimales),
                    'Fecha Creación': fecha_str,
                    'Usuario': ot_data.get('usuario', 'Sin usuario')
                })
    
    df_detalle = pd.DataFrame(detalle_data)
    
    # 3. DataFrame de Evolución Mensual por Clasificación
    evolucion_data = []
    for mes_key in sorted(datos_por_mes.keys()):
        datos_mes = datos_por_mes[mes_key]
        for clasificacion in sorted(datos_mes.keys()):
            ots = datos_mes[clasificacion]
            suma = sum(round(ot.get('valor_declarado', 0),decimales) for ot in ots)
            cantidad = len(ots)
            evolucion_data.append({
                'Mes': mes_key,
                'Clasificación': clasificacion,
                'Suma Total (CLP)': round(suma,decimales),
                'Cantidad': round(cantidad,decimales)
            })
    
    df_evolucion = pd.DataFrame(evolucion_data)
    
    # 4. DataFrame de Top Clasificaciones (agregado total)
    top_data = {}
    for mes_key, datos_mes in datos_por_mes.items():
        for clasificacion, ots in datos_mes.items():
            if clasificacion not in top_data:
                top_data[clasificacion] = {'suma': 0, 'cantidad': 0}
            top_data[clasificacion]['suma'] += sum(round(ot.get('valor_declarado', 0),decimales)  for ot in ots)
            top_data[clasificacion]['cantidad'] += len(ots)
    
    top_list = []
    for clasificacion, datos in top_data.items():
        promedio = datos['suma'] / datos['cantidad'] if datos['cantidad'] > 0 else 0
        top_list.append({
            'Clasificación': clasificacion,
            'Suma Total (CLP)': round(datos['suma'],decimales),
            'Cantidad': datos['cantidad'],
            'Promedio (CLP)': round(promedio,decimales),
            '_suma_num': datos['suma']  # Para ordenamiento
        })    
    df_top = pd.DataFrame(top_list).sort_values('_suma_num', ascending=False).drop(columns=['_suma_num'])
    return {
        't_resumen_reclamos': df_resumen,
        't_detalle_reclamos': df_detalle,
        't_evolucion_reclamos': df_evolucion,
        't_top_clasificaciones_reclamos': df_top
    }


############################################
def crear_resumen_reclamos() -> tuple[bool,list]:
    """Función principal."""
    logger.info("creando resumenes de reclamos")

    # Mostrar filtros aplicados
    if CLASIFICACIONES_EXCLUIDAS:
        logger.info("Clasificaciones excluidas: %s", ", ".join(CLASIFICACIONES_EXCLUIDAS))
    if USUARIOS_EXCLUIDOS:
        logger.info("Usuarios excluidos: %s", ", ".join(USUARIOS_EXCLUIDOS))
    logger.info("MODO: Solo primer registro clasificado por OT (evita duplicados cuando cambia clasificación)")
    
    collection = get_mongo_client()
    # Obtener datos agrupados por mes
    logger.info("Obteniendo datos agrupados por mes y clasificación (primer registro por OT)...")
    datos_por_mes = obtener_datos_por_mes(collection)
    # Generar DataFrames
    logger.info("Generando DataFrames...")
    dataframes = generar_dataframes(datos_por_mes)

    try:
        path_archivos = []
        for df_name,df_value in dataframes.items():
            msn,path_archivo = save_parquet(df=df_value
                                            ,file_name = df_name
                                            ,path_file=settings.reclamos_path
                                            ,save_csv=False)
            if msn:
                path_archivos.append(df_name)
                logger.info(f"Archivos de reclamos {df_name} creado exitosamente")

            else:
                logger.error(f"Archivos de reclamos {df_name} con error")
        
        return True,path_archivos
    
    except Exception as e:
        logger.error(f'Error creando reclamos {e}')
        return False,[e]


# test = read_json_file(settings.config_path)
# tablas_dict = test.get('tablas')

# dict_df = {}

# for valor in tablas_dict:
#     if valor.get('nombre') =='t_resumen_reclamos':
#         print('OK')