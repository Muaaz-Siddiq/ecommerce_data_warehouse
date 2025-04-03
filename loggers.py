import logging

class Logger:
    
    def __init__(self):
        
        logging.basicConfig(
        filename="./ecommerce_logs/ecommerece_pipelining.log",  # Log file name
        level=logging.INFO,  # Logging level
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        datefmt="%Y-%m-%d %H:%M:%S") # Date format
        
        self.logger = logging.getLogger(__name__)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)

logger = Logger()