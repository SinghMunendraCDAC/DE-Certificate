# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 07 : Structured Streaming (Basic)
# MAGIC
# MAGIC Ordinaly, we run workloads in Databricks as a batch. However you can also scale streaming jobs (including data transformation, inferencing, etc) in Apache Spark with Spark Structured Streaming.<br>
# MAGIC
# MAGIC Spark Structed Streaming enables you to manipulate each incoming streams as rows in table. (See below.)
# MAGIC
# MAGIC ![Spark structured streaming](https://tsmatz.github.io/images/github/databricks/20220304_spark_streaming.jpg)
# MAGIC
# MAGIC With this paradigm for incoming streaming, you can then handle streaming data same as a standard dataframe (i.e, table) in batch with consistent programming manners. For instance, you can easily join 2 streaming data by operating ordinary joins for Spark dataframe (stream-stream joins).
# MAGIC
# MAGIC In order to learn the fundamental of Spark Structured Streaming, here we run primitive workloads with file's watcher and memory sinks.<br>
# MAGIC After you've learned the fundamental idea of Structured Streaming, we will proceed to the next exercise, in which we comsume inputs from streaming platforms, such as, Azure Event Hubs or Apache Kafka.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC First, generate json file (file01.json) for preparation.

# COMMAND ----------

dbutils.fs.rm("/tmp/structured-streaming/events", recurse=True)
dbutils.fs.put(
  "/tmp/structured-streaming/events/file01.json",
  """{"event_name":"Open","event_time":1540601000}
{"event_name":"Open","event_time":1540601010}
{"event_name":"Fail","event_time":1540601020}
{"event_name":"Open","event_time":1540601030}
{"event_name":"Open","event_time":1540601040}
{"event_name":"Open","event_time":1540601050}
{"event_name":"Open","event_time":1540601060}
{"event_name":"Fail","event_time":1540601070}
{"event_name":"Open","event_time":1540601080}
{"event_name":"Open","event_time":1540601090}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC See if your file is correctly generated.

# COMMAND ----------

# MAGIC %fs head /tmp/structured-streaming/events/file01.json

# COMMAND ----------

# MAGIC %md
# MAGIC Read this json as Spark streaming dataframe using ```readStream()``` as follows.    
# MAGIC (Unlike previous exercises (batch examples), we use ```readStream()``` instead of ```read()```.)

# COMMAND ----------

# Create streaming dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import *

read_schema = StructType([
  StructField("event_name", StringType(), True),
  StructField("event_time", TimestampType(), True)])
read_df = (
  spark
    .readStream                       
    .schema(read_schema)
    .option("maxFilesPerTrigger", 1)  # one file at a time
    .json("/tmp/structured-streaming/events/")
)

# COMMAND ----------

# MAGIC %md (The following is for debugging purpose and please ignore. This uses normal ```read()``` function instead.)

# COMMAND ----------

# from pyspark.sql.types import *
# from pyspark.sql.functions import *

# test_schema = StructType([
#  StructField("event_name", StringType(), True),
#  StructField("event_time", TimestampType(), True)])
# test_df = (
#  spark
#    .read
#    .schema(test_schema)
#    .json("/tmp/structured-streaming/events/")
# )
# display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we start streaming job by sinking into the memory. This memory data named "```read_simple```" is querable by SQL as streaming query.    
# MAGIC By ```start()``` function, the streaming job is kicked off and continues to run as a background job.
# MAGIC
# MAGIC Note that you **should not use memory sink** in production and this is only for debugging purpose.<br>
# MAGIC In the production system, the processed streaming data should be sinked into data store (such as, file, database, or data warehouse, etc) or other streaming platforms (such as, Apache Kafka, Azure Event Hubs, or Amazon Kinesis, etc).

# COMMAND ----------

# Write results in memory
query1 = (
  read_df
    .writeStream
    .format("memory")
    .queryName("read_simple")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Query sinked data (memory data in this case) with SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, date_format(event_time, "MMM-dd HH:mm") from read_simple order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Add other next events (10 events) into the folder, and then see the previous query's result again.
# MAGIC
# MAGIC > Note : Please see the output (dashboard) in the previous background job, and you will find that the streaming inputs is arriving in near real-time.

# COMMAND ----------

# Add new file in front end and see above result again
dbutils.fs.put(
  "/tmp/structured-streaming/events/file02.json",
  """{"event_name":"Open","event_time":1540601100}
{"event_name":"Open","event_time":1540601110}
{"event_name":"Fail","event_time":1540601120}
{"event_name":"Open","event_time":1540601130}
{"event_name":"Open","event_time":1540601140}
{"event_name":"Open","event_time":1540601150}
{"event_name":"Open","event_time":1540601160}
{"event_name":"Fail","event_time":1540601170}
{"event_name":"Open","event_time":1540601180}
{"event_name":"Open","event_time":1540601190}
""", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, date_format(event_time, "MMM-dd HH:mm") from read_simple order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Next we generate another streaming dataframe with windowing operation.

# COMMAND ----------

# Windowing analysis
group_df = (                 
  read_df
    .groupBy(
      read_df.event_name, 
      window(read_df.event_time, "1 minute"))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Define another streaming sink with query name "read_counts". (Also ```start()``` function kicks off the streaming job and continues to run as a background job.)    
# MAGIC As you can see here, you can define multiple sinks (streaming flows) in Spark Structured Streaming.

# COMMAND ----------

query2 = (
  group_df
    .writeStream
    .format("memory")
    .queryName("read_counts")
    .outputMode("complete")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Query windowing data in memory.

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, date_format(window.end, "MMM-dd HH:mm") as event_time, count from read_counts order by event_time, event_name

# COMMAND ----------

# MAGIC %md
# MAGIC Again, add next 10 events into the folder and see the previous query's result again.

# COMMAND ----------

# Add new file in front end and see above result again
dbutils.fs.put(
  "/tmp/structured-streaming/events/file03.json",
  """{"event_name":"Open","event_time":1540601200}
{"event_name":"Open","event_time":1540601210}
{"event_name":"Fail","event_time":1540601220}
{"event_name":"Open","event_time":1540601230}
{"event_name":"Open","event_time":1540601240}
{"event_name":"Open","event_time":1540601250}
{"event_name":"Open","event_time":1540601260}
{"event_name":"Fail","event_time":1540601270}
{"event_name":"Open","event_time":1540601280}
{"event_name":"Open","event_time":1540601290}
""", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, date_format(window.end, "MMM-dd HH:mm") as event_time, count from read_counts order by event_time, event_name

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, cancel (stop) all previous streaming.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced architecture : Run streaming jobs in scheduled execution
# MAGIC
# MAGIC The streaming job is also used with the combination of batch execution. With this file-based streaming combination, you can also trigger streaming jobs once a day or month in batch execution.<br>
# MAGIC You don't need to run the streaming job on 24/7 (i.e, always on).
# MAGIC
# MAGIC By setting trigger.once execution as follows, the job cluster will automatically stop when it's completed.
# MAGIC
# MAGIC ```
# MAGIC df = spark.readStream \
# MAGIC   .schema(read_schema) \
# MAGIC   .json("path/from/source/dir")
# MAGIC df.writeStream \
# MAGIC   .format("parquet") \
# MAGIC   .option("path", "path/to/destination/dir") \
# MAGIC   .trigger(once=True) \
# MAGIC   .start()
# MAGIC ```
# MAGIC
# MAGIC Next time when you start this job, only the incremental inputs (i.e, delta) are then processed.
# MAGIC
# MAGIC ![batch streaming by trigger once](https://tsmatz.github.io/images/github/databricks/20220304_trigger_once.jpg)
# MAGIC
# MAGIC This architecture is also applied in delta live table's (DLT's) pipeline. (See "Exercise 12 : Delta Live Tables" for details.)
# MAGIC
# MAGIC > Note : For scalable ingestion, you can also use auto-loader by specifying ```cloudFiles``` in format as follows.
# MAGIC > ```
# MAGIC > df = spark.readStream \
# MAGIC >   .format("cloudFiles") \
# MAGIC >   .option("cloudFiles.format", "json") \
# MAGIC >   .schema(read_schema) \
# MAGIC >   .load("path/from/source/dir")
# MAGIC > ```
