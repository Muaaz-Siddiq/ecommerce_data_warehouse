from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loggers import logger
from spark_init import get_spark_session



def create_database(spark: SparkSession, db_name: str) -> None:
    try:
        """
        Create a database For Dataware House.

        Args:
            spark (SparkSession): The Spark session.
            db_name (str): The name of the database to create.
        """
        logger.info(f"Creating database {db_name} if it does not exist.")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        logger.info(f"Database {db_name} created successfully.")
    
    except Exception as e:
        logger.error(f"Error creating database {db_name}: {e}")
        raise e



def create_tables(spark: SparkSession) -> None:
    try:
        """
        Create tables in the database.

        Args:
            spark (SparkSession): The Spark session.
        """
        
        
        logger.info("Creating incremental_value_storage in the database.")
        spark.sql("""
                CREATE TABLE IF NOT EXISTS sale_ecommerce_dwh.incremental_value_storage
                ( 
                id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                start_date DATE,
                incremental_date DATE
                ) USING PARQUET
                """)
        logger.info(" incremental_value_storage Table created successfully.")
        
        
        
        logger.info("Creating Product dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimProducts
                (
                item_id INT,
                sku STRING,
                category STRING,
                DimProductKey INT
                ) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Date dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimDate
                (
                created_at DATE,
                year INT,
                month INT,
                fy STRING,
                DimDateKey INT
                ) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Status dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimStatus 
                (status STRING, DimStatusKey INT) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Payment Method dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimPaymentMethod
                (
                payment_method STRING,
                DimMethodKey INT
                ) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Bi Status dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimBiStatus 
                (bi_status STRING, DimBiStatusKey INT) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Customer dimension tables in the database.")        
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.DimCustomer
                ( 
                customer_id INT,
                customer_since STRING,
                DimCustomerKey INT
                ) USING PARQUET
                """)
        logger.info("Table created successfully.")
        
        
        
        logger.info("Creating Fact Table.")
        spark.sql("""
                CREATE TABLE sale_ecommerce_dwh.FactSalesTable
                    (
                    price DECIMAL (10,2),
                    qty_ordered INT,
                    grand_total DECIMAL (10,2),
                    discount_amount DECIMAL (10,2),
                    mv DECIMAL (10,2),
                    DimProductKey INT,
                    DimDateKey INT,
                    DimStatusKey INT,
                    DimMethodKey INT,
                    DimBiStatusKey INT,
                    DimCustomerKey INT 
                    ) USING PARQUET 
                    """)
        logger.info("Table created successfully.")
    
    
    
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise e




if __name__ == "__main__":
    try:
        logger.info("Starting the database and table creation process.")
        
        spark = get_spark_session()
        logger.info("Spark session started successfully.")
        
        create_database(spark, "sale_ecommerce_dwh")
        create_tables(spark)
    
    except Exception as e:
        logger.error(f"Error in main db_and_tables.py: {e}")
        logger.error("Exiting the program.")
        exit(1)