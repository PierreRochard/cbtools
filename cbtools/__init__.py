import logging
import os
from logging.handlers import RotatingFileHandler

from cbtools.models import SQLAlchemyLogHandler


db_logger = logging.getLogger('database_log')
file_logger = logging.getLogger('database_csv')

db_handler = SQLAlchemyLogHandler()
db_handler.setLevel(logging.INFO)
db_logger.addHandler(db_handler)
db_logger.setLevel(logging.INFO)

log_directory = os.path.realpath(__file__)
log_directory = os.path.abspath(os.path.join(log_directory, '..', '..', 'logs'))
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
log_file = os.path.join(log_directory, 'database_log.csv')
file_handler = RotatingFileHandler(log_file, 'a', 10 * 1024 * 1024, 100)
file_handler.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
file_handler.setLevel(logging.INFO)
file_logger.addHandler(file_handler)
