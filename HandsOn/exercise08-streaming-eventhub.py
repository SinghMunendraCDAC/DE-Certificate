# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 08 : Structured Streaming with Streaming Platform
# MAGIC
# MAGIC In Exercise 07 (Structured Streaming Basic), we have learned the fundamental mechanism of Spark Structured Streaming.<br>
# MAGIC In the practical use for structured streaming, you can combine the following input as streaming data source :
# MAGIC - **Azure Event Hub** (1st-party managed streaming platform in Microsoft Azure)
# MAGIC - **Apache Kafka** (streaming platform developed by open source community, which is also included in Azure HDInsight)
# MAGIC - **Azure Cosmos DB Change Feed** (change data in Azure Cosmos DB, which is better for lambda architecture)
# MAGIC
# MAGIC You can also sink to various destinations for your business needs.<br>
# MAGIC For instance, you can push back the transformed (predicted, windowed, etc) data into Kafka topic or Event Hubs again. (See the following diagrams.)
# MAGIC
# MAGIC ![Structured Streaming](https://tsmatz.github.io/images/github/databricks/20191114_Structured_Streaming.jpg)
# MAGIC
# MAGIC In this exercise, we consume data from Azure Event Hubs in Spark Structured Streaming. (We will then output results into the memory for debugging.)
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparation (Set up Event Hubs and Install library)
# MAGIC Before starting,
# MAGIC 1. Create Event Hubs resource (namespace) in [Azure Portal](https://portal.azure.com/).
# MAGIC 2. Create new Event Hub in previous namespace.
# MAGIC 3. Create SAS policy (shared access policy) on generated Event Hub entity and copy connection string.
# MAGIC 4. Install Event Hub library as follows.
# MAGIC     - Go to "Compute" and select compute (cluster) in Databricks.
# MAGIC     - Click "libraries" tab and install ```com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22``` on Maven repository
# MAGIC     - Restart your compute (cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC Read stream from Azure Event Hubs as streaming inputs with ```readStream()``` as follows.<br>
# MAGIC You must set the connection string for your Event Hub SAS policy in the following placeholder.

# COMMAND ----------

# Read Event Hub's stream
conf = {}
connection_string = "<CONNECTION_STRING_IN_SAS_POLICY>"
# Example : connection_string = "Endpoint=sb://myhub01.servicebus.windows.net/;SharedAccessKeyName=testpolicy01;SharedAccessKey=5sDXk9yYTG...;EntityPath=hub01"
conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)

read_df = (
  spark
    .readStream
    .format("eventhubs")
    .options(**conf)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we start a streaming job, in which the output is sinked into memory and defined as streaming query named "```read_hub```".<br>
# MAGIC The following ```start()``` function kicks off a streaming job and continues to run as a background.

# COMMAND ----------

# Write streams into memory
from pyspark.sql.types import *
import  pyspark.sql.functions as F

read_schema = StructType([
  StructField("event_name", StringType(), True),
  StructField("event_time", StringType(), True)])
decoded_df = read_df.select(F.from_json(F.col("body").cast("string"), read_schema).alias("payload"))

query1 = (
  decoded_df
    .writeStream
    .format("memory")
    .queryName("read_hub")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest (add) events in your Event Hub and see the query result in Spark (Databricks).

# COMMAND ----------

from pyspark.sql import Row

write_schema = StructType([StructField("body", StringType())])
write_row = [Row(body="{\"event_name\":\"Open\",\"event_time\":\"1540601000\"}")]
write_df = spark.createDataFrame(write_row, write_schema)

(write_df
  .write
  .format("eventhubs")
  .options(**conf)
  .save())

# COMMAND ----------

# MAGIC %sql
# MAGIC select payload.event_name, payload.event_time from read_hub

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, stop previous jobs.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------


