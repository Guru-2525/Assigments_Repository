# Databricks notebook source
# MAGIC %md
# MAGIC # Board infinity 
# MAGIC ## Youtube videoes SPARK assignment
# MAGIC ### Done by M.Guruprasad Reddy
# MAGIC ##### In this assignment we performed operations in python , scala , and in some places sql also 

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('BI_Assignment_youtube').getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ##First Import your data in a dataframe

# COMMAND ----------

# Here we are importing the USvideoes data into df dataframe 
df = spark.read.csv("dbfs:/FileStore/tables/USvideos.csv",header=True,inferSchema=True)

# COMMAND ----------

# checking for the datatypes of columns  and understanding the columns . 
df.printSchema()

# COMMAND ----------

# while understanding the data we noticed that every column is in string datatype so we have to convert the data types of few columns 

# COMMAND ----------

#converting some data types as per our convinience 
from pyspark.sql.functions import col
df = df.select(                  col("video_id"),
                                 col("trending_date"),
                                 col("title"),
                                 col("channel_title"),
                                 col("category_id").cast("int"),
                                 col("publish_time"),
                                 col("tags"),
                                 col("views").cast("int"),
                                 col("likes").cast("int"),
                                 col("disliks").cast("int"),
                                 col("comment_count").cast("int"),
                                 col("thumbnail_link"),
                                 col("comments_disabled"),
                                 col("ratings_disabled"),
                                 col("video_error_or_removed"),
                                 col("description"))

# COMMAND ----------

# describing the data to find the is any null values are there  and also to get better understanding of the data 
df.describe().show()


# COMMAND ----------

# MAGIC %scala 
# MAGIC // Here we are importing or reading  the same USvideoes data to RDD df  
# MAGIC val dataRDD = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .format("csv")
# MAGIC   .load("dbfs:/FileStore/tables/USvideos.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Displaying the data 
# MAGIC display(dataRDD)

# COMMAND ----------

# MAGIC %scala
# MAGIC // converting the data types of few columns 
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.types.IntegerType
# MAGIC val modified_df = dataRDD.select(col("video_id"),
# MAGIC                                  col("trending_date"),
# MAGIC                                  col("title"),
# MAGIC                                  col("channel_title"),
# MAGIC                                  col("category_id").cast("int"),
# MAGIC                                  col("publish_time"),
# MAGIC                                  col("tags"),
# MAGIC                                  col("views").cast("int"),
# MAGIC                                  col("likes").cast("int"),
# MAGIC                                  col("dislikes").cast("int"),
# MAGIC                                  col("comment_count").cast("int"),
# MAGIC                                  col("thumbnail_link"),
# MAGIC                                  col("comments_disabled"),
# MAGIC                                  col("ratings_disabled"),
# MAGIC                                  col("video_error_or_removed"),
# MAGIC                                  col("description"))

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Show first 100 records of the dataframe

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.show(100)

# COMMAND ----------

# MAGIC %python
# MAGIC df.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##View the columns of your dataframe and Print schema

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.columns

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.printSchema()

# COMMAND ----------

# MAGIC %python 
# MAGIC df.columns

# COMMAND ----------

# MAGIC %python
# MAGIC df.schema

# COMMAND ----------

# MAGIC %scala
# MAGIC // converting the modified_df RDD into a Temporary view so that we can perform sql operations also .
# MAGIC modified_df.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select few columns - col() & column() & $ sign method & using selectExpr method

# COMMAND ----------

# MAGIC %scala 
# MAGIC val short_data = modified_df.select("video_id","title","channel_title","views","likes","dislikes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT video_id,title,channel_title,views,likes,dislikes
# MAGIC FROM df1

# COMMAND ----------

# Selecting few columns as storing those in another dataframe so that it can be used for further usage .
few_col=df.select("video_id","title","channel_title","views","likes","dislikes")
few_col.show()

# COMMAND ----------

df.selectExpr("video_id","title","channel_title","views","likes","dislikes").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add values to existing value of a column like adding 100 to some integer column

# COMMAND ----------

# MAGIC %scala
# MAGIC val modified_df1 = modified_df.withColumn("Views+100",modified_df("views")+100)
# MAGIC modified_df1.select("Views+100").show()

# COMMAND ----------

df.select(col("views")+100).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select views+100 as views
# MAGIC from df1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add above column to your original DF

# COMMAND ----------

few_col.withColumn("viewsPlus100",few_col["views"]+100).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC short_data.withColumn("viewsplus100",short_data("views")+100).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding a new column named "morelikes" with conditions if likes > dislikes then yes else no

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{when}
# MAGIC short_data.withColumn("morelikes",when(short_data("likes") > short_data("dislikes"),"yes")
# MAGIC                       .when(short_data("likes")<short_data("dislikes"),"no")).show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Above functionality using expr

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.expr

# COMMAND ----------

# MAGIC %scala
# MAGIC short_data.withColumn("morelikes",expr("likes > dislikes")).show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename any column

# COMMAND ----------

# MAGIC %scala 
# MAGIC short_data.withColumnRenamed("channel_title","channel_Name").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performing filter on dataframe using filter() like get only those rows where likes are lesser than dislikes

# COMMAND ----------

df.filter(df["likes"]< df["dislikes"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform same filter on dataframe using where()

# COMMAND ----------

# MAGIC %scala 
# MAGIC short_data.where(short_data("likes")< short_data("dislikes")).show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from df1
# MAGIC where likes < dislikes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add multiple filter conditions

# COMMAND ----------

df.filter((df["likes"]< df["dislikes"])&(df["comment_count"]<"1000")).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df.filter(modified_df("likes")<modified_df("dislikes") && modified_df("comment_count")<"1000").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add multiple filter conditions - filter a string column with some characters - Ex: Take only those rows that don't have NFL in them

# COMMAND ----------

df.filter((df["tags"]).contains("breaking news")).show()


# COMMAND ----------

df.filter((df["title"]).contains("Deleted video")).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Remove Nulls from your data

# COMMAND ----------

# MAGIC %scala
# MAGIC val modified_df_nonull = modified_df.na.drop()

# COMMAND ----------

# MAGIC %scala
# MAGIC modified_df_nonull.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find min and max of views

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(views),max(views)
# MAGIC from df1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find number of videos by channels that are currently trending

# COMMAND ----------

# MAGIC %sql
# MAGIC select channel_title,count(channel_title) as videoes
# MAGIC from df1
# MAGIC group by channel_title
# MAGIC order by videoes desc 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Make your dataframe groupBy category_id along with avg likes and dislikes

# COMMAND ----------

# MAGIC %sql
# MAGIC select category_id,avg(likes),avg(dislikes)
# MAGIC from df1
# MAGIC group by category_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join both Dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC First loading the category file

# COMMAND ----------

# MAGIC %scala 
# MAGIC val j_file = spark.read.option("multiline","true").json("dbfs:/FileStore/tables/US_category_id.json")
# MAGIC j_file.show()

# COMMAND ----------

from pyspark.sql import functions as F
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("readJSON").getOrCreate()
jsondf = spark.read.json("dbfs:/FileStore/tables/US_category_id.json",multiLine=True)
removing_array = jsondf.withColumn('Exp_RESULTS',F.explode(F.col('items'))).drop('items')
json_df = removing_array.select("Exp_RESULTS","Exp_RESULTS.items.*")
json_df.show()

# COMMAND ----------

json_df = json_df.drop("snippet")
json_df.show()

# COMMAND ----------

json_df = json_df.withColumnRenamed("title","title name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join both dataframe on category_id and get category_id and category_name

# COMMAND ----------

joindf = df.join(json_df,df["category_id"] == json_df["id"],how ="left")

# COMMAND ----------

display(joindf.select("category_id","title name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##View all the columns from both dataframe using join

# COMMAND ----------

joined_df = df.join(json_df,df["category_id"] == json_df["id"],how ="left")
joined_df.show()

# COMMAND ----------

joined_df.printSchema()

# COMMAND ----------


