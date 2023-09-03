# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 05 : MLeap
# MAGIC
# MAGIC Essentially MLeap has no dependency on Apache Spark and it has its own core, runtime, and also dataframe (called LeapFrame) including transforms and learners, which corresponds to SparkML objects.
# MAGIC
# MAGIC You can train ML pipeline in Apache Spark and then export the generated pipeline (not only model's parameters, but entire pipeline) into MLeap pipeline format. Afterwards, you will be able to import the serialized pipeline into non-Spark runtime using MLeap framework. (But MLeap doesn't have python binding and then use only scala or java.)
# MAGIC
# MAGIC In this exrcise, we convert Spark ML pipeline (generated in Exercise 03) into MLeap format and then use this MLeap bundle again in Apache Spark.
# MAGIC
# MAGIC > Note : Databricks ML runtime includes MLeap libraries, instead of installing MLeap by yourself.
# MAGIC
# MAGIC > Note : MLeap doesn't always support any Spark ML pipeline format. For instance, ```LightGBMClassifier``` in SynapseML (mmlspark) is not supported in MLeap serialization.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC Same as Exercise 03, we load dataset, convert data, and create a pipeline. (See Exercise03 for details of these code.)
# MAGIC
# MAGIC The generated model (including entire pipeline) is stored in the variable named ```model```.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv"))

from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

df = df.filter(df["DIVERTED"] == 0)

df = df.select(
  "ARR_DEL15",
  "MONTH",
  "DAY_OF_WEEK",
  "UNIQUE_CARRIER",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "RelativeHumidityOrigin",
  "AltimeterOrigin",
  "DryBulbCelsiusOrigin",
  "WindSpeedOrigin",
  "VisibilityOrigin",
  "DewPointCelsiusOrigin",
  "RelativeHumidityDest",
  "AltimeterDest",
  "DryBulbCelsiusDest",
  "WindSpeedDest",
  "VisibilityDest",
  "DewPointCelsiusDest")

df = df.dropna()

(traindf, testdf) = df.randomSplit([0.8, 0.2])

from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
  inputCols = [
    "MONTH",
    "DAY_OF_WEEK",
    "Indexed_UNIQUE_CARRIER",
    "Indexed_ORIGIN",
    "Indexed_DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "RelativeHumidityOrigin",
    "AltimeterOrigin",
    "DryBulbCelsiusOrigin",
    "WindSpeedOrigin",
    "VisibilityOrigin",
    "DewPointCelsiusOrigin",
    "RelativeHumidityDest",
    "AltimeterDest",
    "DryBulbCelsiusDest",
    "WindSpeedDest",
    "VisibilityDest",
    "DewPointCelsiusDest"],
  outputCol = "features")

from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(
  featuresCol="features",
  labelCol="ARR_DEL15",
  maxDepth=15,
  maxBins=500)

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])
model = pipeline.fit(traindf)

pred = model.transform(testdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Create model folder on Spark driver.
# MAGIC
# MAGIC > Note : Do not use dbfs.

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/models
# MAGIC mkdir /tmp/models

# COMMAND ----------

# MAGIC %md
# MAGIC Save and serialize the entire pipeline as MLeap bundle in the previous folder.

# COMMAND ----------

import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
model.serializeToBundle("jar:file:/tmp/models/flight-delay-classify.zip", pred.limit(0))

# COMMAND ----------

# MAGIC %md
# MAGIC As I have described above, you can load MLeap pipeline with MLeap runtime without Apache Spark runtime.<br>
# MAGIC For instance, you can use MLeap serving docker image with Spring Boot framework, and then quickly serve MLeap pipeline on the lightweight container.<br>
# MAGIC For instance, see [here](https://github.com/tsmatz/azureml-examples/blob/master/azureml_spark_serving_by_mleap/azureml_spark_serving_by_mleap.ipynb) for MLeap serving in Azure Machine Learning.
# MAGIC
# MAGIC > Note : For Spark ML model deployment, see Exercise 10 (MLflow).
# MAGIC
# MAGIC However, in this example, we just import MLeap pipeline on Databricks (Spark cluster) again and check the result as follows.

# COMMAND ----------

from pyspark.ml import PipelineModel
loadedPipeline = PipelineModel.deserializeFromBundle("jar:file:/tmp/models/flight-delay-classify.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC Predict data using MLeap pipeline.

# COMMAND ----------

# Predict
pred = loadedPipeline.transform(testdf)

# Compare results
comparelist = pred.select("ARR_DEL15", "prediction")
comparelist.cache()
display(comparelist)

# COMMAND ----------


