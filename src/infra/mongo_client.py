from config.log_config import logger
from config.config import get_settings
from pymongo import MongoClient
from pymongo.errors import PyMongoError

### Configuración
settings = get_settings()


def get_mongo_client(MONGO_URI:str|None = None
                     ,DATABASE_NAME:str|None = None
                     ,COLLECTION_NAME:str|None = None):
    
    if MONGO_URI is None:
        MONGO_URI = settings.mongo_uri_default

    if DATABASE_NAME is None:
        DATABASE_NAME = settings.mongo_db_default

    if COLLECTION_NAME is None:
        COLLECTION_NAME = settings.monog_collection_name_default

    try:
        # Conectar
        client = MongoClient(MONGO_URI,serverSelectionTimeoutMS=60000
                                        ,connectTimeoutMS=60000)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        client.admin.command('ping')
        logger.info("Conectado a %s.%s", DATABASE_NAME, COLLECTION_NAME)
        
        return collection

    except PyMongoError as e:
        logger.error(f'Error MongoDB: {e}')

    except Exception as e:
        logger.exception(f'Error MongoDB: {e}')
