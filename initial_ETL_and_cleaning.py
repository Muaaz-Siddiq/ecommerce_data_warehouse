from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loggers import logger
from spark_init import get_spark_session



def initial_extract_and_cleaning(path:str, file_format:str) -> None:
    try:
        logger.info('Getting spark session...')
        spark = get_spark_session()
        
        
        logger.info('Starting Initial Extract and cleaning...')
        logger.info('Reading Data from source')
        # Reading and extracting data from source
        df = spark.read.format(file_format).options(inferSchema=True, header=True).load(path)
        
        logger.info('Dropping Nulls and Duplicates')
        # Dropping Duplicates and Nulls where whole row is contains Null value
        df = df.dropna(how='all').dropDuplicates()


        logger.info('Dropping Unwanted Columns')
        # Dropping Unwanted Columns
        df = df.drop('_c21', '_c22', '_c23', '_c24', '_c25', 'Working Date', 'sales_commission_code', 'increment_id', 'M-Y')


        logger.info('Adding Unique ID')
        """"
        Using increasing id instead of row_number because every column either has null or garbage value 
        indicating no uniqueness that might shuffle data to fill date columns
        """
        df = df.withColumn('unique_id', monotonically_increasing_id() + 1)



        logger.info('Converting Dates to standard format')
        """ Convert Date to standard format (In Single query we can convert the format as well as place null value instead of garbage so 
        that we can easily identify and replace it.)"""
        df = df.withColumn('created_at', when(to_date(col('created_at'), 'M/d/yyyy').isNotNull(), date_format(to_date(col('created_at'), 
                                            'M/d/yyyy'), 'yyyy-M-d').cast(DateType())).otherwise(None))



        logger.info('Filling Null Dates')
        #fill null value with previous date
        window_forward = Window.orderBy("unique_id").rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn("created_at", last(col("created_at"), ignorenulls=True).over(window_forward))



        logger.info('Filling Null Values for Year and Month')
        # Filling Null Values for Year and Month By extracting from created_at column
        df = df.withColumn("Year", when( col('Year').isNull(), year(col('created_at'))).otherwise(col('Year').cast(IntegerType()))
            ).withColumn('Month',
            when( col('Month').isNull(), month(col('created_at')) ).otherwise(col('Month').cast(IntegerType())) )



        logger.info('Filling FY Column')
        # Filling FY Column with FY + Year from Year column
        # df = df.withColumn('FY', when( col('FY').isNull(), concat( lit('FY') , col('Year'))).otherwise( col('FY') )  )
        df = df.withColumn('FY', concat( lit('FY') , col('Year'))  )



        logger.info('Dropping garbage item_id')
        df = df.filter( (col('item_id').cast(IntegerType())).isNotNull() )



        logger.info('Normalizing Status Columns Data')
        df = df.withColumn('status', regexp_replace(col('status'), 'order_refunded', 'refund' ))



        logger.info('Filling Null Values with mode in Status Column')
        #  Getting the mode value of status column
        col_mode = df.groupBy('status').agg(count('status').alias('count')).orderBy(col('count').desc()).first()[0]

        # Filling null values with mode value
        df = df.withColumn('status', regexp_replace(col('status'), r'\\N', col_mode)).fillna({'status': col_mode})




        logger.info('Normalizing Columns Name')
        df = df.withColumnRenamed('category_name_1', 'category').withColumnRenamed('BI Status', 'bi_status').withColumnRenamed(' MV ', 'mv')\
        .withColumnRenamed('Year', 'year').withColumnRenamed('Month', 'month').withColumnRenamed('FY', 'fy')



        logger.info('Filling Category Column')
        # Creating a dataframe for category from sku column
        category_fill = df.groupBy('sku').agg(first('category').alias('category_filled_values'))

        # Joining the category dataframe with original one to fill the null value and then drop the temporary column
        df = df.join(category_fill, on='sku', how='left').withColumn('category', when( col('category').isNull(), col('category_filled_values')
                                                        ).otherwise( col('category') )).drop('category_filled_values')

        # Filling category and sku column with N/A as no information is available
        df = df.fillna({'sku': 'N/A', 'category': 'N/A'})


        logger.info("Replacing '\\N' with N/A in category")
        df = df.withColumn('category', regexp_replace( col('category'), r'\\N', 'N\A' ))



        logger.info('Dropping rows with testing data')
        df = df.filter( ~col('sku').isin('test-product', 'test-product-00', 'test_tcsconnect') )


        logger.info('Dropping rows where Customer ID is null')
        # Indicating a ambigious data
        df = df.filter( col('Customer ID').isNotNull() )

        # Renaming the customer column
        df= df.withColumnRenamed('Customer Since', 'customer_since').withColumnRenamed('Customer ID', 'customer_id')



        logger.info('Correcting Data Types')
        df = df.withColumn('qty_ordered', col('qty_ordered').cast(IntegerType()) )\
        .withColumn('grand_total', col('grand_total').cast(DoubleType()) )\
        .withColumn('discount_amount', col('discount_amount').cast(DoubleType()) )



        logger.info('Correcting Grand Total')
        df = df.withColumn('grand_total', (col('qty_ordered')*col('price')) - col('discount_amount') )



        logger.info('Assigning new Corrected item_id')
        df = df.withColumn('item_id_new', dense_rank().over(Window.orderBy('sku', 'category')))


        logger.info('Correcting Data convention in sku')
        df = df.withColumn('sku', regexp_replace('sku', '"', ''))


        logger.info('Dropping unique ID')
        df = df.drop('unique_id', 'item_id')



        logger.info('Fixing MV Column')
        df = df.withColumn('mv', regexp_replace('mv', ',', '')).withColumn('mv', col('mv').cast(IntegerType()) )



        logger.info('Order By Date')
        # Order by created_at column for incremantal loading
        df = df.orderBy('created_at')




        logger.info('Saving Data to Parquet Format')
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable('big_sales_ecoms_cleaned')


    except Exception as err:
        logger.error(f"Error occur in initial_extract_and_cleaning function: {err}")
        raise err



if __name__ == "__main__":
    
    try:
        logger.info('Starting Initial ETL Pipeline...')
        # Run the initial extract and cleaning function
        initial_extract_and_cleaning(path = 'path to the data', file_format = 'file_format')
        logger.info('Initial ETL Pipeline Completed Successfully')
    
    except Exception as err:
        logger.error(f"Error occur in ETL Pipeline: {err}")
        exit(1)