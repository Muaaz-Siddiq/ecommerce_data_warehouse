from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loggers import logger

def get_spark_session():
    
    try:
        logger.info('Starting Spark Session...')
        spark = SparkSession.builder.appName("Data_Pipelining").getOrCreate()
        logger.info('Spark Session Started')
        return spark

    except Exception as err:
        logger.error(f"Error occur in spark starting session: {err}")
        raise err