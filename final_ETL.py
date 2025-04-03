from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loggers import logger
from spark_init import get_spark_session


def main_ETL_pipeline() -> None:
    try:
        
        logger.info("Starting ETL pipeline")
        
        
        # Initialize Spark session
        spark = get_spark_session()

        logger.info("Creating and loading staging table")
        # Load the data from the source table based on the incremental value of created_at
        spark.sql("""
                    CREATE OR REPLACE TABLE sale_ecommerce_dwh.stagging_table
                    AS
                    SELECT * FROM big_sales_ecoms_cleaned
                    WHERE created_at BETWEEN (
                    SELECT 
                        CASE
                        WHEN COUNT(*) > 0 THEN (SELECT start_date FROM sale_ecommerce_dwh.incremental_value_storage ORDER BY id DESC LIMIT 1)
                        ELSE '2016-01-01'
                        END AS str_date
                    FROM sale_ecommerce_dwh.incremental_value_storage
                    ) 

                    AND

                    (
                    SELECT 
                        CASE
                        WHEN COUNT(*) > 0 THEN (SELECT incremental_date FROM sale_ecommerce_dwh.incremental_value_storage ORDER BY id DESC LIMIT 1)
                        ELSE '2016-12-31'
                        END AS last_date
                    FROM sale_ecommerce_dwh.incremental_value_storage
                    )
                """)
        
        
        """
        - Extract Incremental Data from stagging table
        - Create View to Transform Data in dimension and their dimenstion (surrogate key)
        - Load Data into Dimenstion Table
        - Managing Slowly Changing Dimension
        """


        logger.info("creating view for dim_product table")
        
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimProducts
                    AS
                    SELECT T.*, row_number() over(ORDER BY T.item_id) as DimProductKey FROM
                    (
                    SELECT
                    DISTINCT(item_id_new) as item_id,
                    sku,
                    category
                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_product table")
        # Load data into dim_product table as per the incremental value and manage slowly changing dimension
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.DimProducts AS trg
                USING sale_ecommerce_dwh.view_DimProducts AS src
                ON trg.item_id = src.item_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for dim_date table")
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimDates
                    AS
                    SELECT T.*, row_number() over(ORDER BY T.created_at) as DimDateKey FROM
                    (
                    SELECT
                    DISTINCT created_at AS created_at,
                    year,
                    month,
                    fy

                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_date table")
        
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.dimdate as trg
                USING sale_ecommerce_dwh.view_dimdates as src
                ON trg.created_at = src.created_at
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for dim_status table")
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimStatus
                    AS
                    SELECT T.* , row_number() OVER (ORDER BY T.status) AS DimStatusKey FROM
                    (
                    SELECT DISTINCT status AS status
                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_status table")
        
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.dimstatus AS trg
                USING sale_ecommerce_dwh.view_dimstatus AS src
                ON trg.status = src.status
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for dim_payment_method table")
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimPaymentMethod
                    AS
                    SELECT T.* , row_number() OVER (ORDER BY T.payment_method) AS DimMethodKey FROM
                    (
                    SELECT DISTINCT payment_method AS payment_method
                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_payment_method table")
        
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.DimPaymentMethod AS trg
                USING sale_ecommerce_dwh.view_DimPaymentMethod AS src
                ON trg.payment_method = src.payment_method
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for dim_bi_status table")
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimBiStatus
                    AS
                    SELECT T.* , row_number() OVER (ORDER BY T.bi_status) AS DimBiStatusKey FROM
                    ( SELECT DISTINCT bi_status AS bi_status
                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_bi_status table")
        
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.DimBiStatus AS trg
                USING sale_ecommerce_dwh.view_DimBiStatus AS src
                ON trg.bi_status = src.bi_status
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for dim_customer table")
        spark.sql("""
                    CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimCustomer
                    AS
                    SELECT T.*, row_number() OVER ( ORDER BY T.customer_id) AS DimCustomerKey
                    FROM (
                    SELECT DISTINCT customer_id AS customer_id, customer_since
                    FROM sale_ecommerce_dwh.stagging_table
                    ) AS T
                    """)
        
        logger.info("Loading data into dim_customer table")
        
        spark.sql("""
                MERGE INTO sale_ecommerce_dwh.DimCustomer AS trg
                USING sale_ecommerce_dwh.view_DimCustomer AS src
                ON trg.customer_id = src.customer_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
        
        
        
        
        logger.info("creating view for fact table")
        spark.sql(""" 
                    CREATE OR REPLACE VIEW view_fact_table
                    AS
                    SELECT 
                    f.price, f.qty_ordered, f.grand_total, f.discount_amount, f.mv, p.DimProductKey, dt.DimDateKey, s.DimStatusKey, m.DimMethodKey, bs.DimBiStatusKey, c.DimCustomerKey
                    FROM sale_ecommerce_dwh.stagging_table AS F

                    LEFT JOIN
                    sale_ecommerce_dwh.dimproducts AS p
                    ON f.item_id_new = p.item_id

                    LEFT JOIN
                    sale_ecommerce_dwh.dimdate AS dt
                    ON f.created_at = dt.created_at

                    LEFT JOIN
                    sale_ecommerce_dwh.dimstatus AS s
                    ON f.status = s.status

                    LEFT JOIN
                    sale_ecommerce_dwh.dimpaymentmethod AS m
                    ON f.payment_method = m.payment_method

                    LEFT JOIN
                    sale_ecommerce_dwh.dimbistatus AS bs
                    ON f.bi_status = bs.bi_status

                    LEFT JOIN
                    sale_ecommerce_dwh.dimcustomer AS c
                    ON f.customer_id = c.customer_id
                """)
        
        logger.info("Loading data into fact table")
        spark.sql("""" 
                    INSERT INTO sale_ecommerce_dwh.FactSalesTable
                    SELECT * FROM view_fact_table
                """)
        
        
        
        
        logger.info("Updating incremental value storage")
        # Update the incremental value storage table with 365 days from the last date
        spark.sql(""""
                    INSERT INTO sale_ecommerce_dwh.incremental_value_storage ( start_date, incremental_date)
                    SELECT
                    CASE 
                        WHEN COUNT(*) > 0 THEN (SELECT MAX(incremental_date) FROM sale_ecommerce_dwh.incremental_value_storage)
                        ELSE date('2017-01-01')
                    END,

                    CASE 
                        WHEN COUNT(*) > 0 THEN (SELECT DATE_ADD(MAX(incremental_date), 365) FROM sale_ecommerce_dwh.incremental_value_storage)
                        ELSE date('2017-12-31')
                    END

                    FROM sale_ecommerce_dwh.incremental_value_storage
                """)
        
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as err:
        logger.error(f"Error occurred in ETL pipeline: {err}")
        raise err



if __name__ == "__main__":
    try:
        logger.info("Starting final ETL pipeline")
        
        main_ETL_pipeline()
        
        logger.info("Final ETL pipeline completed successfully")
    
    except Exception as err:
        logger.error(f"Error occurred in final ETL pipeline: {err}")
        exit(1)
