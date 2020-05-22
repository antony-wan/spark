'''
Lectures followed on Datacamp
'''
####################################
######### Data cleaning ############
####################################
'''
Exemples :
-wrong data type
-range for numerical values
-representation of unknown/incomplete data
-absence des valeurs
-column naming conentions
-regional datatimes VS UTC
'''

import pyspark.sql.types #in order to enforce scheme
from pyspark.sql import SparkSession

#To load a dataset with PySPark, we must create a SparkSession
spark = SparkSession.builder.getOrCreate()

#read csv
prices = spark.read.options(header="true").csv('....csv')

#Enforcing a scheme. If not, all the columns from the reading of a csv are considered as as String
schema = StructType([StructField('store', StringType(), nullable=False),
                     StructField('countrycode', StringType(), nullable=False),
                     StructField('price', FloatType(), nullable=False)])
prices = spark.read.options(header='true').schema(schema).csv('.csv')

#Handle invalid rows
prices = spark.read.options(header='true', mode = "DROPMALFORMED").schema(schema).csv('.csv')

#For incomplete data, pyspark substitued with NULL
#Otherwise, supplying defualt values for missing dataCSV
prices.fillna(25,subset=['quantity']) #All the null values from 'quantity' are substitued by 25

#Conditionally replace values
from  pyspark.sql.functions import col, when
from datetime import date, timedelta
one_year_from_now = date.today().replace(year=date.today().year + 1)
better_frame = employees.withColumn('end_date', when(col('end_date') > one_year_from_now, None). otherwise(col('end_date')))
better_frame.show()


#####################################
######### Transform data ############
#####################################
'''
Exemples
-standardize hotel name
-normalizing review score

Common data transformation
-filtering rows
-selecting and rename ColumnSelector
-grouping and aggregation
-joining multiple dataset
-ordering result
'''
#filtering and ordering rows
prices_in_belgium = prices.filter(col('countrycode'=='BE')).orderBy(col('date'))

#Select, renaming columns and reducing duplicate values
from pyspark.sql.functions import col
prices.select(
col('store'),
col('brand').alias('brandname')
).distinct()

#Grouping and aggregating with mean()
prices.groupBy(col('brand)).mean('prices')

#Grouping and aggregating with agg()
(prices
    .groupBy(col('brand'))
    .agg(
        avg('prices').alias('average_price'),
        count('brand').alias('number_of_items')
    )
).show()

#Join table. Executing a join with 2 foreign keys
ratings_with_prices = ratings.join(prices, ['brand', 'model'])
