# Databricks notebook source
# MAGIC %md
# MAGIC # To Do
# MAGIC
# MAGIC - Replace placefolders in the first cell.
# MAGIC - Replace placeholders in the last cell.

# COMMAND ----------

df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/<FILL-SUB-DIRECTORY-NAME>/*"))

# COMMAND ----------

# MAGIC %md Select required columns

# COMMAND ----------

df = df.select(
  "Year",
  "Month",
  "DayofMonth",
  "DayOfWeek",
  "UniqueCarrier",
  "Origin",
  "Dest",
  "CRSDepTime",
  "CRSArrTime",
  "DepDelay",
  "ArrDelay",
  "Cancelled",
  "Diverted")

# COMMAND ----------

# MAGIC %md Remove rows with null value for required columns

# COMMAND ----------

df.dropna(how="any", subset=("ArrDelay","Cancelled","Diverted"))

# COMMAND ----------

# MAGIC %md Convert auto-detected column types to appropriate types

# COMMAND ----------

from pyspark.sql.types import *
df = df.withColumn("CancelledFlag", df["Cancelled"].cast(BooleanType()))
df = df.drop("Cancelled")
df = df.withColumn("DivertedFlag", df["Diverted"].cast(BooleanType()))
df = df.drop("Diverted")

# COMMAND ----------

# MAGIC %md Add new column which tells whether arrival time is depalyed over 15 miniutes or not.

# COMMAND ----------

from pyspark.sql.functions import when
df = df.withColumn("ArrDelay15Flag", when(df["ArrDelay"] >= 15, True).otherwise(False))

# COMMAND ----------

# MAGIC %md ArrDelay15 is true if it's canceled.

# COMMAND ----------

df = df.withColumn("ArrDelay15Flag", when(df["CancelledFlag"] == True, True).otherwise(df["ArrDelay15Flag"]))

# COMMAND ----------

# MAGIC %md Remove flights if it's diverted.

# COMMAND ----------

df = df.filter(df["DivertedFlag"] == False)

# COMMAND ----------

### For Debugging
# display(df)

# COMMAND ----------

# MAGIC %md Save converted dataframe as parquet

# COMMAND ----------

df.write.parquet("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flights_parquet", mode="overwrite")

# COMMAND ----------


