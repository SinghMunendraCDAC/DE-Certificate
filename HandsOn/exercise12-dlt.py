# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 12 : Delta Live Tables (DLT)
# MAGIC
# MAGIC In previous exercise (Exercise 11 : Orchestration with Azure Data Services), we have built ETL pipeline with Azure managed services - Data Factory.<br>
# MAGIC Delta Live Tables (DLT) is built-in ETL framework in Databricks, and you can manage pipeline for multiple tables (such as, star schema data model) with a simple declarative approach.
# MAGIC
# MAGIC > Note : Delta Live Tables (DLT) requires the Premium plan.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC In this exercise, you already have Delta Live Tables (DLT) declaration in [Exercise12/dlt-definition](https://tsmatz.github.io/azure-databricks-exercise/Exercise12/dlt-definition.html) notebook.
# MAGIC
# MAGIC The source code in this notebook is below.<br>
# MAGIC As you can see below, 3 DLT tables - ```test01```, ```test02```, and ```test03``` - are defined.
# MAGIC
# MAGIC - The table ```test01``` stores raw data from source (json files in ```/tmp/source/events```).
# MAGIC - The table ```test02``` stores cleaned data, in which all missing rows are removed.
# MAGIC - The table ```test03``` stores summary data, in which data is aggregated in each 1 minute window.
# MAGIC
# MAGIC ```
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC read_schema = StructType([
# MAGIC   StructField("event_name", StringType(), True),
# MAGIC   StructField("event_time", TimestampType(), True)])
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="raw stream table"
# MAGIC )
# MAGIC def test01():
# MAGIC   return (
# MAGIC     # load incrementally
# MAGIC     spark.readStream
# MAGIC       .format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "json")
# MAGIC       .schema(read_schema)
# MAGIC       .load("/tmp/source/events/")
# MAGIC   )
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="cleaned table"
# MAGIC )
# MAGIC def test02():
# MAGIC   return (
# MAGIC     # update incrementally
# MAGIC     dlt.read_stream("test01")
# MAGIC       .dropna()
# MAGIC   )
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="summary table"
# MAGIC )
# MAGIC def test03():
# MAGIC   # recompute completely
# MAGIC   read_df = dlt.read("test02")
# MAGIC   return (
# MAGIC     read_df
# MAGIC       .groupBy(
# MAGIC         read_df.event_name, 
# MAGIC         window(read_df.event_time, "1 minute"))
# MAGIC       .count()
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC > Note : This architecture, in which raw tables (bronze tables), cleaned tables (silver tables), and summary tables (gold tables) are provisioned, is called Medallion Architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's store data in source location.

# COMMAND ----------

dbutils.fs.rm("/tmp/source/events", recurse=True)
dbutils.fs.put(
  "/tmp/source/events/file01.json",
  """{"event_name":"Open","event_time":1540601000}
{"event_name":"Open","event_time":1540601010}
{"event_name":"Fail","event_time":1540601020}
{"event_name":"Open","event_time":1540601030}
{"event_name":"Open","event_time":1540601040}
{"event_time":1540601050}
{"event_name":"Open","event_time":1540601060}
{"event_name":"Fail","event_time":1540601070}
{"event_time":1540601080}
{"event_name":"Open","event_time":1540601090}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC See if your file is correctly generated.

# COMMAND ----------

# MAGIC %fs head /tmp/source/events/file01.json

# COMMAND ----------

# MAGIC %md
# MAGIC Go to "Workflows" menu and select "Delta Live Tables" tab in Databricks. Then, click "Create Pipeline" to create a pipeline for DLT.
# MAGIC
# MAGIC In this creation wizard, :
# MAGIC
# MAGIC - Select "Core" as "product edtion"
# MAGIC - Select ```Exercise12/dlt-definition``` in notebook
# MAGIC - Select "Triggered" as "pipeline mode"
# MAGIC - Fill "dlt_test" in "Target schema"<br>
# MAGIC (Set blank in "Storage location".)
# MAGIC
# MAGIC ![Create DLT Pipeline](https://tsmatz.github.io/images/github/databricks/20220624_dlt_pipeline.jpg)
# MAGIC
# MAGIC After the settings, press "Create" button to complete.

# COMMAND ----------

# MAGIC %md
# MAGIC Press "Start" to start pipeline.<br>
# MAGIC This will automatically create a job cluster for DLT (Delta Live Tables) and it will then take a while. (Please wait for competion.)
# MAGIC
# MAGIC After completion, you will see the following pipeline graph.
# MAGIC
# MAGIC ![Table Result 1](https://tsmatz.github.io/images/github/databricks/20220624_dlt_graph.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Go to "Data" menu and select "dlt_test" database.<br>
# MAGIC You will be able to see 3 tables - ```test01```, ```test02```, and ```test03```.
# MAGIC
# MAGIC Please check as follows.
# MAGIC
# MAGIC - Missing 2 events (see above data) was dropped in ```test02```.
# MAGIC - Total 8 events are counted in ```test03```. (See below.)
# MAGIC
# MAGIC ![Table Result 1](https://tsmatz.github.io/images/github/databricks/20220624_table_result01.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC We are using ```cloudFiles``` format to use auto-loader. (See above code.)<br>
# MAGIC The auto-loader allows **incremental data ingestion** from a variety of data sources by ```readStream()``` API. Furthermore, it can apply trigger.once (which I have mentioned in [Exercise 07](https://tsmatz.github.io/azure-databricks-exercise/exercise07-structured-streaming.html)) by specifying Triggered mode in DLT. (See [here](https://databricks.com/discover/pages/getting-started-with-delta-live-tables) for details.)
# MAGIC
# MAGIC For incremental ingestion, now let's add new json data in source location as follows.

# COMMAND ----------

dbutils.fs.put(
  "/tmp/source/events/file02.json",
  """{"event_name":"Open","event_time":1540601100}
{"event_name":"Open","event_time":1540601110}
{"event_name":"Fail","event_time":1540601120}
{"event_time":1540601130}
{"event_name":"Open","event_time":1540601140}
{"event_name":"Open","event_time":1540601150}
{"event_name":"Open","event_time":1540601160}
{"event_name":"Fail","event_time":1540601170}
{"event_name":"Open","event_time":1540601180}
{"event_name":"Open","event_time":1540601190}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Go to "Workflows" and start above DLT pipeline again.<br>
# MAGIC When pipeline is completed, you can see 8 rows and total 17 events in ```test03``` as follows.
# MAGIC
# MAGIC ![Table Result 1](https://tsmatz.github.io/images/github/databricks/20220624_table_result02.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC > Note : You can also use high-level **Change Data Capture (CDC)** capability in Delta Live Tables. (Please use ```Pro``` or ```Advanced``` compute for using CDC.)
