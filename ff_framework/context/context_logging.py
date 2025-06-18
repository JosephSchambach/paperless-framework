import logging
from typing import Literal, Optional

class ContextLogger:
    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
    def log(self, message: str, level: str = Optional[Literal['info', 'error', 'warning', 'debug']], data = None):
        if data is not None:
            message += f" | Data: {data}"
        if level is None:
            level = 'info'
        if level not in ['info', 'error', 'warning', 'debug']:
            raise ValueError("Invalid log level. Use 'info', 'error', 'warning', or 'debug'.")
        print(message)
        if level == 'info':
            self.logger.info(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'debug':
            self.logger.debug(message)
