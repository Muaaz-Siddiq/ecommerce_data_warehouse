# Databricks notebook source
# MAGIC %md
# MAGIC DATA PRE-PROCESSING CLEANING 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC read file

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/Pakistan_Largest_Ecommerce_Dataset.csv', inferSchema=True, header=True)
# df.limit(4).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Nulls and duplicates

# COMMAND ----------

# print('Count before Dropping', df.count())
df = df.dropna(how='all').dropDuplicates()
# print('Count after Dropping', df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC checking for nulls in columns

# COMMAND ----------

df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping columns as it contains only nulls

# COMMAND ----------

df = df.drop('_c21', '_c22', '_c23', '_c24', '_c25')

# COMMAND ----------

# MAGIC %md
# MAGIC Check for both the date columns to verfiy either they are duplicate columns with different name

# COMMAND ----------

print('Total row', df.count())
# df.select('created_at', 'Working Date').display()

matching_count = df.filter(
    (col('created_at') == col('Working Date')) | (col('Working Date').isNull() )
).count()

print('Total rows date columns match', matching_count)

# COMMAND ----------

# not_match = df.select('created_at', 'Working Date', '').filter(
#     (col('created_at') != col('Working Date')) | (col('Working Date').isNull() )
# ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Working date as it is duplicate column as created_at beside some garbage values

# COMMAND ----------

df = df.drop('Working Date', 'sales_commission_code', 'increment_id')

# COMMAND ----------

# MAGIC %md 
# MAGIC replacing garbage values in created at

# COMMAND ----------

# MAGIC %md
# MAGIC Since to fill null dates wrt to its previous date
# MAGIC 1. we need to define a new column with ids so that we can perform window function
# MAGIC 2. apply window function with unbound method to fill dates
# MAGIC
# MAGIC WARNING: Fill Dates with caution and as per businees requirement as it is one of the most critical factor in sales data

# COMMAND ----------

""""
Using increasing id instead of row_number because every column either has null or garbage value 
indicating no uniqueness that might shuffle data to fill date columns
"""

# df = df.withColumn('unique_id', row_number().over(Window.orderBy('item_id')))
df = df.withColumn('unique_id', monotonically_increasing_id() + 1)

# COMMAND ----------

df.select('created_at', 'unique_id').orderBy('created_at').display()

# COMMAND ----------

# Convert Date to standard format (In Single query we can convert the format as well as place null value instead of garbage so that we can easily identify and replace it.)

df = df.withColumn('created_at',
             when(
                 to_date(col('created_at'), 'M/d/yyyy').isNotNull(), date_format(to_date(col('created_at'), 'M/d/yyyy'), 'yyyy-M-d').cast(DateType())).otherwise(None)
             )


# COMMAND ----------

# window_forward = Window.orderBy("unique_id").rowsBetween(Window.unboundedPreceding, 0)
# test.withColumn("created_at", 
#                   last(col("created_at"), ignorenulls=True).over(window_forward)).select('created_at', 'unique_id').orderBy('created_at').filter(col('unique_id').isin(17179873210, 17179873209)).display()

# COMMAND ----------


#fill null value with previous date
window_forward = Window.orderBy("unique_id").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("created_at", 
                  last(col("created_at"), ignorenulls=True).over(window_forward))

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if we can fil null years and month with M-Y column before dropping it

# COMMAND ----------

# df.filter( (col('Year').isNull()) | (col('Month').isNull()) ).select('Year', 'Month', 'M-Y').display()

# COMMAND ----------

df = df.drop('M-Y')

# COMMAND ----------

df = df.withColumn("Year",
              when( col('Year').isNull(), year(col('created_at'))).otherwise(col('Year').cast(IntegerType()))
              ).withColumn('Month',
                when( col('Month').isNull(), month(col('created_at')) ).otherwise(col('Month').cast(IntegerType())) )




# COMMAND ----------

df.filter(col('Year').isNull()).select('Year', 'Month').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Filling FY column

# COMMAND ----------

df = df.withColumn('FY',
        when( col('FY').isNull(), concat( lit('FY') , col('Year'))).otherwise( col('FY') )  )    
              

# df.filter( col('FY').isNull() ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC checking for garbage row
# MAGIC common trend: row with garbage item_id most probably contains wrong data

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.filter( (col('item_id').cast(IntegerType())).isNull() )

# COMMAND ----------

df = df.filter( (col('item_id').cast(IntegerType())).isNotNull() )

# df.display(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Normalizing columns data

# COMMAND ----------

# df.select('status').distinct().display()
df.select('sku', 'category_name_1').filter(col('category_name_1').isin('\\N', None)).display()

# COMMAND ----------

# df.groupBy('status').agg(count('status').alias('count')).orderBy(col('count').desc()).display()

# Extract mode
col_mode = df.groupBy('status').agg(count('status').alias('count')).orderBy(col('count').desc()).first()[0]

# df.groupBy('category_name_1').agg(count('category_name_1').alias('count')).orderBy(col('count').desc()).display()

# COMMAND ----------

df = df.withColumn('status',
              regexp_replace(col('status'), 'order_refunded', 'refund' ))

df = df.withColumn('status',
              regexp_replace(col('status'), r'\\N', col_mode)).fillna({'status': col_mode})

# COMMAND ----------

# df.filter(col('status').isNull()).display()
df = df.withColumnRenamed('category_name_1', 'category').withColumnRenamed('BI Status', 'bi_status').withColumnRenamed(' MV ', 'mv')\
    .withColumnRenamed('Year', 'year').withColumnRenamed('Month', 'month').withColumnRenamed('FY', 'fy')

# COMMAND ----------

df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).display()

# COMMAND ----------

category_fill = df.groupBy('sku').agg(first('category').alias('category_filled_values'))
# category_fill.display()

# COMMAND ----------

df = df.join(category_fill, on='sku', how='left').withColumn('category', 
                                                        when( 
                                                             col('category').isNull(), col('category_filled_values')
                                                        ).otherwise( col('category') )
                                                        ).drop('category_filled_values')


# COMMAND ----------

# df.select( 'item_id', 'sku', 'category').filter( col('sku').isNull() ).display()
# items_ids = df.select('item_id').filter( col('sku').isNull() ).collect()
# items_check = df.select('item_id', 'sku').filter( col('item_id').isin( [itemId.item_id for itemId in items_ids ] ) ).display()


df = df.fillna({'sku': 'N/A', 'category': 'N/A'})

# COMMAND ----------

df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping all the rows where Customer ID is null
# MAGIC   - similiar null values in different column indicate that all the other columns might be null as well where customer id is null.

# COMMAND ----------

df.filter( col('Customer ID').isNull() ).select('Customer ID', 'Customer Since', 'mv', 'bi_status', 'payment_method', 'price', 'qty_ordered', 'discount_amount', 'grand_total').display()

# COMMAND ----------

df = df.filter( col('Customer ID').isNotNull() )
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Correct the Column types and Names

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn('qty_ordered', col('qty_ordered').cast(IntegerType()) )\
    .withColumn('grand_total', col('grand_total').cast(DoubleType()) )\
    .withColumn('discount_amount', col('discount_amount').cast(DoubleType()) )

# COMMAND ----------

df= df.withColumnRenamed('Customer Since', 'customer_since').withColumnRenamed('Customer ID', 'customer_id')

# COMMAND ----------

df = df.drop('unique_id')

# COMMAND ----------

df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).display()

# COMMAND ----------

df = df.withColumn('mv', regexp_replace('mv', ',', '')).withColumn('mv', col('mv').cast(IntegerType()) )

# df.limit(1).display()

# COMMAND ----------

df = df.orderBy('created_at')
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Saving cleaned data to file for further transformation and loading

# COMMAND ----------

df.write.mode("overwrite").saveAsTable('big_sales_ecoms_cleaned')

# COMMAND ----------

# logger.info('Starting Initial Extract and cleaning...')

# logger.info('Reading Data from source')
# Reading and extracting data from source
df = spark.read.csv('/FileStore/tables/Pakistan_Largest_Ecommerce_Dataset.csv', 
                    inferSchema=True, header=True)

# logger.info('Dropping Nulls and Duplicates')
# Dropping Duplicates and Nulls where whole row is contains Null value
df = df.dropna(how='all').dropDuplicates()


# logger.info('Dropping Unwanted Columns')
# Dropping Unwanted Columns
df = df.drop('_c21', '_c22', '_c23', '_c24', '_c25', 'Working Date', 'sales_commission_code', 'increment_id', 'M-Y')


# logger.info('Adding Unique ID')
""""
Using increasing id instead of row_number because every column either has null or garbage value 
indicating no uniqueness that might shuffle data to fill date columns
"""
df = df.withColumn('unique_id', monotonically_increasing_id() + 1)



# logger.info('Converting Dates to standard format')
""" Convert Date to standard format (In Single query we can convert the format as well as place null value instead of garbage so 
that we can easily identify and replace it.)"""
df = df.withColumn('created_at', when(to_date(col('created_at'), 'M/d/yyyy').isNotNull(), date_format(to_date(col('created_at'), 
                                    'M/d/yyyy'), 'yyyy-M-d').cast(DateType())).otherwise(None))



# logger.info('Filling Null Dates')
#fill null value with previous date
window_forward = Window.orderBy("unique_id").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("created_at", last(col("created_at"), ignorenulls=True).over(window_forward))



# logger.info('Filling Null Values for Year and Month')
# Filling Null Values for Year and Month By extracting from created_at column
df = df.withColumn("Year", when( col('Year').isNull(), year(col('created_at'))).otherwise(col('Year').cast(IntegerType()))
    ).withColumn('Month',
    when( col('Month').isNull(), month(col('created_at')) ).otherwise(col('Month').cast(IntegerType())) )



# logger.info('Filling FY Column')
# Filling FY Column with FY + Year from Year column
# df = df.withColumn('FY', when( col('FY').isNull(), concat( lit('FY') , col('Year'))).otherwise( col('FY') )  )
df = df.withColumn('FY', concat( lit('FY') , col('Year'))  )



# logger.info('Dropping garbage item_id')
df = df.filter( (col('item_id').cast(IntegerType())).isNotNull() )



# logger.info('Normalizing Status Columns Data')
df = df.withColumn('status', regexp_replace(col('status'), 'order_refunded', 'refund' ))



# logger.info('Filling Null Values with mode in Status Column')
#  Getting the mode value of status column
col_mode = df.groupBy('status').agg(count('status').alias('count')).orderBy(col('count').desc()).first()[0]

# Filling null values with mode value
df = df.withColumn('status', regexp_replace(col('status'), r'\\N', col_mode)).fillna({'status': col_mode})




# logger.info('Normalizing Columns Name')
df = df.withColumnRenamed('category_name_1', 'category').withColumnRenamed('BI Status', 'bi_status').withColumnRenamed(' MV ', 'mv')\
.withColumnRenamed('Year', 'year').withColumnRenamed('Month', 'month').withColumnRenamed('FY', 'fy')



# logger.info('Filling Category Column')
# Creating a dataframe for category from sku column
category_fill = df.groupBy('sku').agg(first('category').alias('category_filled_values'))

# Joining the category dataframe with original one to fill the null value and then drop the temporary column
df = df.join(category_fill, on='sku', how='left').withColumn('category', when( col('category').isNull(), col('category_filled_values')
                                                ).otherwise( col('category') )).drop('category_filled_values')

# Filling category and sku column with N/A as no information is available
df = df.fillna({'sku': 'N/A', 'category': 'N/A'})


# logger.info('Replacing '\\N' with N/A in category)
df = df.withColumn('category', regexp_replace( col('category'), r'\\N', 'N\A' ))



# logger.info('Dropping rows with testing data')
df = df.filter( ~col('sku').isin('test-product', 'test-product-00', 'test_tcsconnect') )


# logger.info('Dropping rows where Customer ID is null')
# Indicating a ambigious data
df = df.filter( col('Customer ID').isNotNull() )

# Renaming the customer column
df= df.withColumnRenamed('Customer Since', 'customer_since').withColumnRenamed('Customer ID', 'customer_id')



# logger.info('Correcting Data Types')
df = df.withColumn('qty_ordered', col('qty_ordered').cast(IntegerType()) )\
.withColumn('grand_total', col('grand_total').cast(DoubleType()) )\
.withColumn('discount_amount', col('discount_amount').cast(DoubleType()) )



# logger.info('Correcting Grand Total')
df = df.withColumn('grand_total', (col('qty_ordered')*col('price')) - col('discount_amount') )



# logger.info('Assigning new Corrected item_id')
df = df.withColumn('item_id_new', dense_rank().over(Window.orderBy('sku', 'category')))


# logging.info('Correcting Data convention in sku')
df = df.withColumn('sku', regexp_replace('sku', '"', ''))


# logger.info('Dropping unique ID')
df = df.drop('unique_id', 'item_id')



# logger.info('Fixing MV Column')
df = df.withColumn('mv', regexp_replace('mv', ',', '')).withColumn('mv', col('mv').cast(IntegerType()) )



# logger.info('Order By Date')
# Order by created_at column for incremantal loading
df = df.orderBy('created_at')




# logger.info('Saving Data to Parquet Format')
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable('big_sales_ecoms_cleaned')