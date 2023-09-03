# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 03 : Spark Machine Learning Pipeline
# MAGIC
# MAGIC In this exercise, we'll see more practical machine learning example using **Spark ML pipeline**.    
# MAGIC
# MAGIC In this example, we create a model to predict the flight delay over 15 minutes (```ARR_DEL15```) using other features - such as, airport, career, and corresponding weather conditions in airport.
# MAGIC
# MAGIC Before starting, run "Exercise 01 : Storage Settings", because we use [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your blob container.
# MAGIC
# MAGIC > Note : For simplicity, here I use built-in DecisionTree Classifier in MLlib, but you can use LightGBM classifier in SynapseML library (formerly MMLSpark library) for more accurate results. 
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC First, we load flight's history information (over 400 MB), which is used for training.

# COMMAND ----------

# Read dataset from Azure Blob
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC Show data in this notebook.<br>
# MAGIC In this exercise, we use the following columns.
# MAGIC
# MAGIC - ```ARR_DEL15``` : 1 when the flight is delayed over 15 minutes, 0 otherwise.
# MAGIC - ```MONTH```, ```DAY_OF_WEEK``` : Date information (Month and Day of week).
# MAGIC - ```UNIQUE_CARRIER``` : Airline carrier, such as, "AA" (American Airlines), "DL" (Delta Air Lines), etc.
# MAGIC - ```ORIGIN``` : Departure airport code.
# MAGIC - ```DEST``` : Destination airport code.
# MAGIC - ```CRS_DEP_TIME``` : Departure time.
# MAGIC - ```CRS_ARR_TIME``` : Destination time.
# MAGIC - ```XXXOrigin``` : Weather conditions in departure airport.
# MAGIC - ```XXXDest``` : Weather conditions in destination airport.

# COMMAND ----------

# See data
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Mark as "delayed over 15 minutes" if it's canceled.

# COMMAND ----------

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove flights if it's diverted.

# COMMAND ----------

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Narrow to required columns.

# COMMAND ----------

# Select required columns
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

# COMMAND ----------

# MAGIC %md
# MAGIC Drop rows which has null value in columns.

# COMMAND ----------

# Drop rows with null value
df = df.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC Split data into training data and evaluation data (ratio is 80% : 20%).

# COMMAND ----------

# Split data into train data and test data
(traindf, testdf) = df.randomSplit([0.8, 0.2])

# COMMAND ----------

# MAGIC %md
# MAGIC Convert categorical scala to index values (0, 1, ...) for the following columns.
# MAGIC
# MAGIC - Carrier code (```UNIQUE_CARRIER```)
# MAGIC - Airport code in departure (```ORIGIN```)
# MAGIC - Airport code in destination (```DEST```)
# MAGIC - Flag (0 or 1) for delay over 15 minutes (```ARR_DEL15```)

# COMMAND ----------

# Convert categorical values to index values (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

# COMMAND ----------

# MAGIC %md
# MAGIC In Spark machine learning, the feature columns must be wrapped as a single vector value.    
# MAGIC This new column (vector column) is named as "features".

# COMMAND ----------

# Assemble feature columns
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

# COMMAND ----------

# MAGIC %md
# MAGIC Generate classifier. Here we use Decision Tree classifier.
# MAGIC
# MAGIC > Note : You can also use more performant LightGBM classifier in Spark. (See above note.)

# COMMAND ----------

# Generate classifier
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol="features", labelCol="ARR_DEL15", maxDepth=15, maxBins=500)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate SparkML pipeline and run training.<br>
# MAGIC ML pipeline defines a sequence of stages, and in this exercice, the following 6 stages are run in this pipeline.
# MAGIC
# MAGIC - Convert into index for ```UNIQUE_CARRIER``` column
# MAGIC - Convert into index for ```ORIGIN``` column
# MAGIC - Convert into index for ```DEST``` column
# MAGIC - Convert into index for ```ARR_DEL15``` column
# MAGIC - Assemble into a single vector for features
# MAGIC - Apply classifier
# MAGIC
# MAGIC The trained model (with coefficients) and pipeline are then stored in "```model```".

# COMMAND ----------

# Create pipeline and Train
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])
model = pipeline.fit(traindf)

# COMMAND ----------

# MAGIC %md
# MAGIC Predict data (eveluation data) with the trained pipeline.

# COMMAND ----------

# Predict with eveluation data
pred = model.transform(testdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Show eveluation metrics.
# MAGIC
# MAGIC > Note : As I have mentioned above, it might not be good result, because we simply use primitive decision classifier. (Use LightGBM classifier for more accurate result.)

# COMMAND ----------

# Evaluate results
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction")
accuracy = evaluator.evaluate(pred)
print("Accuracy = %g" % accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC Save (Export) pipeline model with trained coefficients.    
# MAGIC Then saved pipeline model can be loaded for inference, such as, using Azure Machine Learning or Databricks model inference. (See Exercise 10 for details.)
# MAGIC
# MAGIC Before running, **you must run Exercise 01, because we use the mounted folder** (```/mnt/testblob```).

# COMMAND ----------

# Save pipeline
model.write().overwrite().save("/mnt/testblob/flight_model")
