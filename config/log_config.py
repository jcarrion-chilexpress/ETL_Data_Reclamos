#config/log_config.py
import logging
import logging.handlers
from pathlib import Path
from config.config import get_settings


## -------------------------------------------------- ##
######################################################################
class registroLOG:
    @staticmethod
    def setup_logging(
        log_name: str | None = get_settings().archivo_log,
        log_level=logging.INFO
    ):
        log_name = log_name or "app"

        logs_dir = Path(__file__).resolve().parents[1] / 'logs'
        logs_dir.mkdir(exist_ok=True)

        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)

        if not logger.handlers:
            formatter = logging.Formatter(
                    '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            file_handler = logging.handlers.RotatingFileHandler(
                filename=logs_dir / f'{log_name}.log',
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger


logger = registroLOG.setup_logging()
