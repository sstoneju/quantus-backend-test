from datetime import datetime
from loguru import logger

today = datetime.today()
logger.add(f'system_log_{today.year}_{today.month}_{today.day}.log')