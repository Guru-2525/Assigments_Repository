// Databricks notebook source
// MAGIC %scala
// MAGIC import spark.implicits._
// MAGIC import java.sql.Timestamp
// MAGIC // here we are creating a structure of the dataframe which contains column names and type also .
// MAGIC 
// MAGIC case class Data(
// MAGIC   Timestamp:String,                       ReportType:String,	
// MAGIC   Target:String,	                      Referrer:String,	
// MAGIC   Link:String,	                          SessionId:String,	
// MAGIC   SessionCount:Long,                      PageTitle:String,	
// MAGIC   LoadTime:String,	                      ViewTime:String,	
// MAGIC   Embedded:String,	                      Cookie:String,	
// MAGIC   HSResponseTime:String,	              PrefetchElement:String,	
// MAGIC   ElementsinHints:String,	              HintAlreadySeen:String, 
// MAGIC   Viewedfor1sttimePrefetched:String,	  Viewed1sttimenotPrefetched:String,	
// MAGIC   ConxSpeed:String,	                      ConxType:String,	
// MAGIC   PrevConxType:String,	                  VisitstoOrder:String,	
// MAGIC   DaystoOrder:String,	                  VisitFreq:String,	
// MAGIC   PurchaseFreq:String,	                  VisChip:String,	
// MAGIC   TimeinSession:String,	                  PreprocRules:String,	
// MAGIC   Secondssincelastpage:String, 	          ScreenResolution:String,	
// MAGIC   ColorDepth:String,	                  CookiesEnabled:String,	
// MAGIC   ReferringURL:String,	                  Product1stVisit:String,	
// MAGIC   FlashVersion:String,	                  UserAgent:String,	
// MAGIC   RemoteIP:String,	                      Serial:String,	
// MAGIC   TargetMatches:String,	                  NormalizedTarget:String,	
// MAGIC   ThirdPartyCookieEnabled:String )

// COMMAND ----------

//Creating a RDD and reading the text_file into it  then assining the previously defined column names and converting to dataframe .
val DataDF = spark.sparkContext
                   .textFile("/FileStore/tables/Web_Analytic_Dataset.txt") // reding the text data
                   .map(_.split("}"))                                      //giving condition how to divide rows . 
                   .map(attributes => Data(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), 
                            attributes(6).toInt, attributes(7), attributes(8), attributes(9), attributes(10), 
                            attributes(11), attributes(12), attributes(13), attributes(14), attributes(15), 
                            attributes(16), attributes(17), attributes(18), attributes(19), attributes(20), 
                            attributes(21), attributes(22), attributes(23), attributes(24), attributes(25), 
                            attributes(26), attributes(27), attributes(28), attributes(29), attributes(30), 
                            attributes(31), attributes(32), attributes(33), attributes(34), attributes(35), 
                            attributes(36), attributes(37), attributes(38), attributes(39), attributes(40))).toDF()   //assigning the columns


// COMMAND ----------

display(DataDF)

// COMMAND ----------


// converting the pre defined dataframe to a temp View so the we can query in SQL also.
DataDF.createOrReplaceTempView("data")

// COMMAND ----------

// MAGIC %md
// MAGIC ## SOME PRACTICE QUERIES

// COMMAND ----------

// DBTITLE 1,Get the timing and data in a understandable way from Timestamp column
// MAGIC %sql
// MAGIC select Timestamp,from_unixtime(Timestamp,"dd-MM-yyyy")as date,from_unixtime(Timestamp,"H")as time_hours
// MAGIC from data 

// COMMAND ----------

// DBTITLE 1, How many target websites that the data contains ?
// MAGIC %sql
// MAGIC select distinct(Target)
// MAGIC from data

// COMMAND ----------

// DBTITLE 1,Which target website is visited mostly ?
// MAGIC %sql
// MAGIC SELECT Target,count(Target)as COUNT
// MAGIC FROM data
// MAGIC GROUP BY Target
// MAGIC ORDER BY COUNT DESC

// COMMAND ----------

// DBTITLE 1,List the referrer websites in the data .
// MAGIC %sql
// MAGIC SELECT DISTINCT Referrer
// MAGIC FROM data

// COMMAND ----------

// DBTITLE 1,Each referrer has how many target visits ?
// MAGIC %sql
// MAGIC SELECT Referrer,COUNT (Target) as number
// MAGIC FROM data
// MAGIC GROUP BY Referrer
// MAGIC ORDER BY number DESC

// COMMAND ----------

// DBTITLE 1,For how many users cookies are enabled 
// MAGIC %sql
// MAGIC SELECT CookiesEnabled,COUNT(CookiesEnabled)
// MAGIC FROM data 
// MAGIC GROUP BY CookiesEnabled

// COMMAND ----------

// DBTITLE 1,Session count per every hour
// MAGIC %sql
// MAGIC SELECT from_unixtime(Timestamp,"HH")as HOUR ,sum(SessionCount)
// MAGIC FROM data
// MAGIC GROUP BY HOUR

// COMMAND ----------

// DBTITLE 1,What is the total "sessioncount" and "viewTime" ? is the any relation between them
// MAGIC %sql 
// MAGIC SELECT  PageTitle , sum(SessionCount)as Total_sessioncount, sum(viewTime) as Total_viewtime
// MAGIC FROM data
// MAGIC GROUP BY PageTitle

// COMMAND ----------

display(DataDF)

// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,What is the internet connection Speed of the customers ?
// MAGIC %sql
// MAGIC SELECT ConxSpeed as connection_speed,count(ConxSpeed) as total_customers
// MAGIC FROM data 
// MAGIC GROUP BY connection_speed
// MAGIC ORDER By total_customers DESC

// COMMAND ----------

// DBTITLE 1,what are the ways that customers are doing the payment ? 
// MAGIC %sql
// MAGIC SELECT VisChip as pay_method ,count(VisChip) as Count
// MAGIC FROM data
// MAGIC GROUP BY VisChip
// MAGIC ORDER BY Count

// COMMAND ----------

// DBTITLE 1,Category wise sales
// MAGIC %sql
// MAGIC SELECT PageTitle as  category ,sum(split(NormalizedTarget,"/")[1]) as sales 
// MAGIC FROM data
// MAGIC GROUP BY PageTitle
// MAGIC ORDER BY sales DESC

// COMMAND ----------

// DBTITLE 1,New visitors per HOUR
// MAGIC %sql
// MAGIC SELECT from_unixtime(Timestamp,"HH")as Time,count(Viewedfor1sttimePrefetched)
// MAGIC FROM data
// MAGIC where Viewedfor1sttimePrefetched ="YES"
// MAGIC GROUP BY Time
// MAGIC ORDER BY Time

// COMMAND ----------


