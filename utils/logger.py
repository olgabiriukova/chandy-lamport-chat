import logging
from logging.handlers import RotatingFileHandler
import os
import sys

def make_logger(node_id: str, lamport_clock, log_file: str = None):
    logger_name = f"node_{node_id.replace(':', '_')}"
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        def _log(msg: str):
            ts = lamport_clock.read()
            logger.info(f"{node_id} | {msg}", extra={'lamport': ts})
        return _log
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(
        '%(asctime)s [L%(lamport)d] %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    if log_file:
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.info(f"Log started for node {node_id}", extra={'lamport': 0})
        except Exception as e:
            print(f"Error creating log file {log_file}: {e}", file=sys.stderr)
    
    def _log(msg: str):
        ts = lamport_clock.read()
        logger.info(f"{node_id} | {msg}", extra={'lamport': ts})
    
    return _log

def async_logger(node_id: str, lamport_clock, log_file: str = None):
    sync_log = make_logger(node_id, lamport_clock, log_file)
    async def _alog(msg: str):
        sync_log(msg)
    
    return _alog