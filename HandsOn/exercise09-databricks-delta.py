# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 09 : Delta Lake
# MAGIC
# MAGIC Delta Lake is the optimized storage layer built on top of parquet format with transaction tracking (journals).<br>
# MAGIC While it keeps optimized performance of parquet (columnar compression format), it also realizes the following capabilities by using state information (journals).
# MAGIC
# MAGIC - Provides isolation level (ACID transaction) which avoid conflicts.
# MAGIC - Enable snapshots for data with time travel or versioning.
# MAGIC - Skip data (files), which is not referred by query, using Z-Ordering.
# MAGIC - Optimize the layout of data (including Z-Ordering) for querying whole set of data. (Small file compactions)<br>
# MAGIC You can also handle workloads for both streaming and batch with a single storage layout. (It's called Kappa architecture.)
# MAGIC - You can soon remove data (vacuum), which is no longer needed.
# MAGIC - Support MERGE command. (e.g, Support efficient upserts.)
# MAGIC - Prevent polluting tables with dirty data. (Schema enforcement)
# MAGIC
# MAGIC > Note : Delta Lake requires Databricks Runtime 4.1 or above.
# MAGIC
# MAGIC > Note : Delta table has some constraints compared with normal parquet format. See [here](https://docs.databricks.com/delta/delta-faq.html) for the supported operations.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Delta table (table in Delta Lake) as follows. All you have to do is to just say "delta" instead of "parquet".
# MAGIC
# MAGIC > Note : All tables on Azure Databricks are Delta tables by default.

# COMMAND ----------

dbutils.fs.rm("/tmp/delta", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS testdb;
# MAGIC USE testdb;
# MAGIC DROP TABLE IF EXISTS testdb.table01;
# MAGIC CREATE TABLE testdb.table01(
# MAGIC   event_id INT,
# MAGIC   event_name STRING,
# MAGIC   event_time TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/tmp/delta/events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimized for both streaming and batch
# MAGIC
# MAGIC In this section, let's see how it's optimized for both streaming and batch.

# COMMAND ----------

# MAGIC %md
# MAGIC First, we create source data (json) in files.

# COMMAND ----------

dbutils.fs.rm("/tmp/structured-streaming/events", recurse=True)
dbutils.fs.put(
  "/tmp/structured-streaming/events/file_org.json",
  """{"event_id":0,"event_name":"Open","event_time":1540601000}
{"event_id":1,"event_name":"Open","event_time":1540601010}
{"event_id":2,"event_name":"Fail","event_time":1540601020}
{"event_id":3,"event_name":"Open","event_time":1540601030}
{"event_id":4,"event_name":"Open","event_time":1540601040}
{"event_id":5,"event_name":"Open","event_time":1540601050}
{"event_id":6,"event_name":"Open","event_time":1540601060}
{"event_id":7,"event_name":"Fail","event_time":1540601070}
{"event_id":8,"event_name":"Open","event_time":1540601080}
{"event_id":9,"event_name":"Open","event_time":1540601090}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we write streaming data (json) into previous Delta table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Write data without streaming into delta table
# read_schema = StructType([
#  StructField("event_id", IntegerType(), False),
#  StructField("event_name", StringType(), True),
#  StructField("event_time", TimestampType(), True)])
# df1 = spark.read.schema(read_schema).json("/tmp/structured-streaming/events")
# df1.write.mode("overwrite").format("delta").save("/tmp/delta/events")

# Streaming reads and append into delta table (Start !)
read_schema = StructType([
  StructField("event_id", IntegerType(), False),
  StructField("event_name", StringType(), True),
  StructField("event_time", TimestampType(), True)])
df2 = (spark.readStream
  .option("maxFilesPerTrigger", "1")
  .schema(read_schema)
  .json("/tmp/structured-streaming/events"))
(df2.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/delta/checkpoint")
  .option("path", "/tmp/delta/events").start())

# COMMAND ----------

# MAGIC %md
# MAGIC Insert additional 100 rows one by one.<br>
# MAGIC These rows will be fragmented in Delta table.

# COMMAND ----------

import random
for x in range(100):
  event_id = 10 + x
  rnd = random.randint(0,1)
  event_name="Open"
  if rnd == 1:
    event_name="Fail"
  rnd = random.randint(-30000000,30000000)
  event_time = 1540601000 + rnd
  dbutils.fs.put(
    "/tmp/structured-streaming/events/file%d.json" % (x),
    "{\"event_id\":%d,\"event_name\":\"%s\",\"event_time\":%d}" % (event_id, event_name, event_time),
    True)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until all 110 records (rows) are generated ...

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC Now query events in Delta table.<br>
# MAGIC All data are fragmented and this query will then need a little bit time to complete. (The time to be consumed will be displayed in the following output.)<br>
# MAGIC
# MAGIC > Note : Please run this query multiple times, because the query will be evaluated in the first time execution.

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, event_time from testdb.table01 where event_name = "Open" order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Run ```OPTIMIZE``` command.    
# MAGIC This command compacts a lot of small files into large ones to speed up. Furthermore this command improves the performance for batch query by specifing ```ZORDER``` clustering.<br>
# MAGIC When there exist massively large data, only required partitions are referred by query and other partitions will be skipped and ignored.
# MAGIC
# MAGIC > Note : You can also set **automatic optimization (auto optimize)** specifying a property ```delta.autoOptimize = true``` in table.

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE testdb;
# MAGIC OPTIMIZE "/tmp/delta/events"
# MAGIC ZORDER BY (event_name, event_time)

# COMMAND ----------

# MAGIC %md
# MAGIC Now query data again, and see the consumed time for this query.<br>
# MAGIC You could find it's more performant than the previous result.

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_name, event_time from testdb.table01 where event_name = "Open" order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time traveling
# MAGIC
# MAGIC In this section, let's see time traveling and rollback in Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting, see the number of records of "Open" events.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC Now we corrupt our data intentionally !

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE testdb.table01 SET event_name = 'Fail' WHERE event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC By running the above command, now you can see that no data is "Open" events.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC In Delta Lake, the updated state and data are recorded in version history.<br>
# MAGIC Please pick up the version number of latest correct data.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC Rollback data with the version of correct data. (Please replace the following version number to your appropriate value.)
# MAGIC
# MAGIC > Note : You can also get history data using timestamp, instead of version number.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO testdb.table01 dst
# MAGIC USING testdb.table01 VERSION AS OF 102 src
# MAGIC ON src.event_id = dst.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# MAGIC %md
# MAGIC Check if data is restored.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC With ```VACUUM``` command, you can remove files that are no longer in the latest state in the transaction logs. (i.e, It cleans up the history.)<br>
# MAGIC Note that there exists a retention period (7 days by default), and it cannot then be immediately removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, stop previous streaming job.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists testdb cascade

# COMMAND ----------


