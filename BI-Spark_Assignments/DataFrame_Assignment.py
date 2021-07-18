# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('BI_Assignment').getOrCreate()

# COMMAND ----------

#Here we are creating a dataframe and loasing the stpcl.csv data into it. 
#In the below code we have given header as true and inferSchema also --> wht it will do is it will take first row as header and it will verify which column is which datatype.
df = spark.read.csv("/FileStore/tables/walmart_stock.csv", header=True, inferSchema=True)
#here we are asking to how to data .
display(df)

# COMMAND ----------

df.take(5)
# df.head(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.head(5)

# COMMAND ----------

for row in df.head(5):
  print(row)

# COMMAND ----------

df.describe().show()


# COMMAND ----------

df.describe().printSchema()

# COMMAND ----------

from pyspark.sql.functions import format_number

# COMMAND ----------

result = df.describe()

# COMMAND ----------

result.select(result['summary'], 
              format_number(result['Open'].cast('float'), 2).alias('Open'),
             format_number(result['High'].cast('float'), 2).alias('High'),
             format_number(result['Low'].cast('float'), 2).alias('Low'),
             format_number(result['Close'].cast('float'), 2).alias('Close'),
             format_number(result['Volume'].cast('int'),0).alias('Volume')).describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a new column called HV Ratio which is the ratio of High Price versus volume of stocks traded per day

# COMMAND ----------

df2 = df.withColumn("HV Ratio", df["High"]/df["Volume"])

df2.select('HV Ratio').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #What day had the Peak High in Price?

# COMMAND ----------

Day_high = df.orderBy(df['High'].desc()).head(1)
display(Day_high)

# COMMAND ----------

# MAGIC %md
# MAGIC #What is the mean of the close column?

# COMMAND ----------

from pyspark.sql.functions import mean

df.select(mean("Close")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #What is the max and min of Volume column?

# COMMAND ----------

from pyspark.sql.functions import max , min 

df.select(max("Volume") ,min("Volume")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##How many days was the close lower than 60 dollars?

# COMMAND ----------

df.where(df["close"]<60).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What percentage of the time was the high greater than 80 dollars? 
# MAGIC ## i.e. (Number of days high > 80)/(Total days in the dataset)

# COMMAND ----------

Tot_high_count = df.select(df["High"]).count()
HIGH_80= df.where(df["High"]>80).count()
(HIGH_80/Tot_high_count)*100

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##What is the max high per year?

# COMMAND ----------

df.createOrReplaceTempView("df2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select year(Date) as year ,max(High) as max_of_year
# MAGIC FROM df2
# MAGIC group by year
# MAGIC order by year

# COMMAND ----------

# MAGIC %md
# MAGIC ##What is the average Close for each calender month?
# MAGIC 
# MAGIC ##In other words, across the years, what is the average close price for Jan, Feb, Mar... Your result will have value for each month

# COMMAND ----------

from pyspark.sql.functions import month
monthdf = df.withColumn('Month', month('Date'))

# COMMAND ----------

monthavgs = monthdf.select('Month', 'Close').groupBy('Month').mean()

# COMMAND ----------

monthavgs.select('Month', 'avg(Close)').orderBy('Month').show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT month(Date) as  month , avg(Close) as avg_close
# MAGIC FROM df2
# MAGIC group by  month
# MAGIC order by month

# COMMAND ----------

# DBTITLE 1,Practice Questions


# COMMAND ----------

# MAGIC %md
# MAGIC ##  how many days was the close lesser than open ? 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Date,Open,Close
# MAGIC FROM df2
# MAGIC WHERE  Open > Close

# COMMAND ----------

# MAGIC %md
# MAGIC ## The above table shows that on which days the stock opened at high price and closed in less price

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## print the date and open fields where open and close are equal 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Date,Open
# MAGIC FROM df2
# MAGIC WHERE  Open == Close

# COMMAND ----------


