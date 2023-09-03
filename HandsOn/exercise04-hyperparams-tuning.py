# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 04 : Hyper-parameter Tuning
# MAGIC
# MAGIC In the practical machine learning, it’s very hard to find best parameters - such as, learning rare, number of epochs, [regularization parameters](https://tsmatz.wordpress.com/2017/09/13/overfitting-for-regression-and-deep-learning/), so on and so forth. In Spark machine learning, you can quickly find best parameters by using Spark distributed manners.
# MAGIC
# MAGIC In this exercise, we change the source code of Exercise 03 and find the best values of parameters - "maxDepth" and "maxBins" - for decision tree classifier. (Here we use grid search for parameter's sweep.)
# MAGIC
# MAGIC > Note : Here we explore only classification's parameters, but you can also tune transformation's parameters.
# MAGIC
# MAGIC Before starting, run "Exercise 01 : Storage Settings", because we use [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your blob container.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC Same as Exercise 03, we load dataset, convert data, and create a pipeline. (See Exercise03 for details of these code.)

# COMMAND ----------

# Read dataset
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv"))

# COMMAND ----------

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# COMMAND ----------

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

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

# Drop rows with null values
df = df.dropna()

# COMMAND ----------

# Convert categorical values to indexer (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

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

# Define classifier
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol="features", labelCol="ARR_DEL15")

# COMMAND ----------

# Create pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])

# COMMAND ----------

# MAGIC %md
# MAGIC Now we apply hyper-parameter sweep for the generated pipeline.<br>
# MAGIC The following execution will take a long time, because it uses a serial evaluation by default. Use ```setParallelism()``` to improve performance in production.
# MAGIC
# MAGIC > Note : You can also use ```CrossValidator()``` instead of using ```TrainValidationSplit()```, but please be care for training overheads when using ```CrossValidator()```.    
# MAGIC The word "Cross Validation" means : For example, by setting ```numFolds=5``` in ```CrossValidator()```, 4/5 is used for training and 1/5 is for testing, and moreover each pairs are replaced and averaged. As a result, 5 pairs of dataset are used and the training occurs 3 params x 3 params x 5 pairs = 45 times. (See "[ML Tuning: model selection and hyperparameter tuning](https://spark.apache.org/docs/latest/ml-tuning.html)" in official Spark document.)

# COMMAND ----------

# Run pipeline with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

### 3 x 3 = 9 times training occurs (it takes a long time)
paramGrid = ParamGridBuilder() \
 .addGrid(classifier.maxDepth, [10, 20, 30]) \
 .addGrid(classifier.maxBins, [300, 400, 500]) \
 .build()
### for debugging (for more short time)
# paramGrid = ParamGridBuilder() \
#   .addGrid(classifier.maxDepth, [10, 15]) \
#   .addGrid(classifier.maxBins, [251, 300]) \
#   .build()

# Set appropriate parallelism by setParallelism() in production
# (It takes a long time)
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation
model = tvs.fit(df)

# COMMAND ----------

# MAGIC %md
# MAGIC List results for all sweeped parameters.

# COMMAND ----------

# View all results (accuracy) by each params
list(zip(model.validationMetrics, model.getEstimatorParamMaps()))

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we try to predict using best model.

# COMMAND ----------

df10 = df.limit(10)
model.bestModel.transform(df10)\
  .select("ARR_DEL15", "prediction")\
  .show()
