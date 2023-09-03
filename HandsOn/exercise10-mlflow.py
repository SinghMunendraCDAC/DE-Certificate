# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 10 : MLflow
# MAGIC
# MAGIC MLflow provides end-to-end lifecycle management, such as logging (tracking), model deployment, and automating MLflow project by MLflow CLI.<br>
# MAGIC Here we run the script of Exercise 04 with MLflow integrated tracking and autologging, and deploy Spark ML pipeline model (for serving) with MLflow.
# MAGIC
# MAGIC 1. Databricks already includes managed MLflow server and you can soon integrate with your project in MLflow (without any extra setup). First, we'll run MLflow API with native **Databricks backend**.<br>
# MAGIC 2. MLflow can also run tracking, deployment, and automating project on Azure Machine Learning backend (see [here](https://github.com/tsmatz/mlflow-azureml) for details) and we'll run the same MLflow API with **Azure Machine Learning backend** in this later half of examples.
# MAGIC
# MAGIC Before starting, run "Exercise 01 : Storage Settings", because we use [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your blob container.
# MAGIC
# MAGIC Before starting, do the following settings in your cluster.
# MAGIC
# MAGIC > Note : Databricks Autologging is enabled by default on clusters running Databricks Runtime 10.2 ML and above, and we use this mechanism by enabling MLflow PySpark ML autologging (```mlflow.pyspark.ml.autolog()```) in this exercise.
# MAGIC > Otherwise (runtime 5.4 - 10.1), MLlib-automated MLflow tracking is enabled on clusters instead, and you should then remove ```mlflow.pyspark.ml.autolog()``` in the following source code.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Train and Track Metric with MLflow
# MAGIC
# MAGIC Same as Exercise 04, we load dataset, convert data, and create a pipeline, except for the following note (setting ```metricName``` property) with bold fonts.

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
# MAGIC **Now we set ```metricName``` property in evaluator, and this metirc is logged by MLflow.**

# COMMAND ----------

# Prepare training with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
### 4 x 2 = 8 times training occurs (it takes a long time)
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 15, 20]) \
  .addGrid(classifier.maxBins, [251, 300]) \
  .build()
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction", metricName="weightedPrecision"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC Now start training with MLflow tracking. (The metric tracking is started by ```mlflow.start_run()```.)<br>
# MAGIC Here we also log the best precision and save the best pipeline model file.
# MAGIC
# MAGIC > Note : As I have mentioned in Exercise 04, use ```setParallelism()``` in ```TrainValidationSplit``` to improve performance. By default, it runs on a serial evaluation and takes a long time.

# COMMAND ----------

# Start mlflow and run pipeline
import mlflow
from mlflow import spark
mlflow.pyspark.ml.autolog() # (Remove here when you run on old version of ML runtime. See above.)
with mlflow.start_run() as parent_run:
  model = tvs.fit(df)                                               # logs above metric (weightedPrecision)
  mlflow.log_metric("bestPrecision", max(model.validationMetrics)); # logs user-defined custom metric
  mlflow.spark.log_model(model.bestModel, "model-file")             # logs model as artifacts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. See Logs with MLflow UI
# MAGIC
# MAGIC Open MLFlow experiments in this notebook and click top-right icon ("View Experiment UI") to view MLflow experiment UI.
# MAGIC
# MAGIC ![Click Runs](https://tsmatz.github.io/images/github/databricks/20190620_View_Runs.jpg)
# MAGIC
# MAGIC The following MLflow Experiment UI is then opened.<br>
# MAGIC You can view a top run (which includes above ```bestPrecision```) and 8 child runs (each of which includes above ```weightedPrecision```) as follows. (total 9 runs)
# MAGIC
# MAGIC ![Experiment UI](https://tsmatz.github.io/images/github/databricks/20200423_experiment_ui.jpg)
# MAGIC
# MAGIC In experiment UI, set ```params.DecisionTreeClassifier.maxBins = 300``` in "Search Runs" box and click "Search" button. Then you can filter results and total 4 runs are listed as follows.    
# MAGIC Now select all filtered results (select all check-boxes) and click "Compare" button.
# MAGIC
# MAGIC ![Select and Compare](https://tsmatz.github.io/images/github/databricks/20220201_filter_compare.jpg)
# MAGIC
# MAGIC In the next screen, select ```DecisionTreeClassifier.maxDepth``` on X-axis and ```weightedPrecision``` on Y-axis in scatter plot area.    
# MAGIC Then you can view the effect of ```DecisionTreeClassifier.maxDepth``` for performance (accuracy) using a scatter plot as follows.
# MAGIC
# MAGIC ![Scatter Plot](https://tsmatz.github.io/images/github/databricks/20220201_scatter_plots.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. See Logs with Tracking API
# MAGIC
# MAGIC See logs with tracking API (not UI) as follows.
# MAGIC
# MAGIC > Note : User Databricks Runtime 5.1 and above, because we show pandas dataframe using ```display()``` here.

# COMMAND ----------

all_runs = mlflow.search_runs(max_results=10)  # Note : This is pandas dataframe
display(all_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform run logs and you can show data in visuals as you need.

# COMMAND ----------

child_runs = all_runs.dropna(subset=["metrics.weightedPrecision"])
display(child_runs[["params.DecisionTreeClassifier.maxBins","params.DecisionTreeClassifier.maxDepth","metrics.weightedPrecision"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register and Deploy Model
# MAGIC
# MAGIC The model is also saved on MLflow.<br>
# MAGIC When you click the top level of runs (parent run), you can see the saved artifacts (in this case, pipeline model) named "```model-file```" in MLflow UI.
# MAGIC
# MAGIC ![MLFlow Artifacts](https://tsmatz.github.io/images/github/databricks/20200423_saved_artifacts.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC With MLflow API, register model in model registry on Databricks.

# COMMAND ----------

mlflow.register_model(
  "runs:/{}/model-file".format(parent_run.info.run_id),
  "testmodel01")

# COMMAND ----------

# MAGIC %md
# MAGIC Select "Machine Learning" in the sidebar and click "Models".<br>
# MAGIC You can then see a registered model, named ```testmodel01```.
# MAGIC
# MAGIC ![model registry](https://tsmatz.github.io/images/github/databricks/20220704_model_menu.jpg)
# MAGIC
# MAGIC Select this model and go to "Serving" tab.<br>
# MAGIC In this pane, click "Enable Serving".
# MAGIC
# MAGIC ![enable serving button](https://tsmatz.github.io/images/github/databricks/20220704_enable_serving.jpg)
# MAGIC
# MAGIC After new job cluster (in which, Apache Spark runs) is started, the conda environent will be created and the model is then deployed here.<br>
# MAGIC Wait till both cluster status and deployment status are ready. (See below.)
# MAGIC
# MAGIC ![status in screen](https://tsmatz.github.io/images/github/databricks/20220704_serving_status.jpg)
# MAGIC
# MAGIC Now you can invoke this realtime inference endpoint (REST API).<br>
# MAGIC Click "Query endpoint" and fill the following json in "Request" textbox and push "Send Request" button in test tool.
# MAGIC
# MAGIC ```
# MAGIC {"dataframe_records" : [
# MAGIC     {
# MAGIC         "ARR_DEL15": 0,
# MAGIC         "MONTH": 1,
# MAGIC         "DAY_OF_WEEK": 1,
# MAGIC         "UNIQUE_CARRIER": "DL",
# MAGIC         "ORIGIN": "ATL",
# MAGIC         "DEST": "EWR",
# MAGIC         "CRS_DEP_TIME": 8,
# MAGIC         "CRS_ARR_TIME": 10,
# MAGIC         "RelativeHumidityOrigin": 93.0,
# MAGIC         "AltimeterOrigin": 30.0175,
# MAGIC         "DryBulbCelsiusOrigin": 12.05,
# MAGIC         "WindSpeedOrigin": 7.5,
# MAGIC         "VisibilityOrigin": 1.0,
# MAGIC         "DewPointCelsiusOrigin": 11.025,
# MAGIC         "RelativeHumidityDest": 96.5,
# MAGIC         "AltimeterDest": 30.055,
# MAGIC         "DryBulbCelsiusDest": 4.5,
# MAGIC         "WindSpeedDest": 6.0,
# MAGIC         "VisibilityDest": 0.5,
# MAGIC         "DewPointCelsiusDest": 3.95
# MAGIC     },
# MAGIC     {
# MAGIC         "ARR_DEL15": 0,
# MAGIC         "MONTH": 1,
# MAGIC         "DAY_OF_WEEK": 1,
# MAGIC         "UNIQUE_CARRIER": "AA",
# MAGIC         "ORIGIN": "ABQ",
# MAGIC         "DEST": "DFW",
# MAGIC         "CRS_DEP_TIME": 11,
# MAGIC         "CRS_ARR_TIME": 14,
# MAGIC         "RelativeHumidityOrigin": 34.0,
# MAGIC         "AltimeterOrigin": 30.16,
# MAGIC         "DryBulbCelsiusOrigin": -2.8,
# MAGIC         "WindSpeedOrigin": 15.0,
# MAGIC         "VisibilityOrigin": 10.0,
# MAGIC         "DewPointCelsiusOrigin": -16.7,
# MAGIC         "RelativeHumidityDest": 80.5,
# MAGIC         "AltimeterDest": 30.25,
# MAGIC         "DryBulbCelsiusDest": 0.8,
# MAGIC         "WindSpeedDest": 9.5,
# MAGIC         "VisibilityDest": 10.0,
# MAGIC         "DewPointCelsiusDest": -2.1
# MAGIC     }
# MAGIC ]}
# MAGIC ```
# MAGIC You can then see the HTTP response body.<br>
# MAGIC In my case, it returns ```[1,0]``` as follows. It means that the first row is predicted as "Delayed", and the latter one is "Not Delayed".
# MAGIC
# MAGIC ![HTTP request and response](https://tsmatz.github.io/images/github/databricks/20220704_serving_invoke.jpg)
# MAGIC
# MAGIC This endpoint is published as protected HTTP endpoint, and you can then invoke this endpoint with various fashions. (For instance, when you call with Python script, click "Python" and copy code in above screen.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Track MLflow logs in Azure Machine Learning
# MAGIC
# MAGIC You can also save your MLflow logs in [Azure Machine Learning](https://tsmatz.wordpress.com/2018/11/20/azure-machine-learning-services/).
# MAGIC
# MAGIC Before starting, please prepare as follows.
# MAGIC
# MAGIC 1. Create new Azure Machine Learning resource in [Azure Portal](https://portal.azure.com/).    
# MAGIC After you've created your AML workspace, please copy your workspace name, resource group name, and subscription id.    
# MAGIC ![Copy attributes](https://tsmatz.github.io/images/github/databricks/20190710_AML_Workspace.jpg)
# MAGIC 2. Install Azure Machine Learning Python SDK for Databricks in your cluster as follows
# MAGIC     - On your cluster setting, select "Libraries" tab.
# MAGIC     - Install ```azureml-mlflow==1.42.0``` on "PyPI" source.
# MAGIC     - Restart your cluster
# MAGIC
# MAGIC > Note : In order to connect to an Azure Machine Learning workspace, you can also use the following "**Link Azure ML Workspace**" button in Azure Databricks resource blade, with which the tracked logs will be sent into both Databricks workspace and Azure Machine Learning workspace (dual-tracking experience).<br>
# MAGIC > With this simplified experience, you don't need to run ```Workspace.get()``` and ```mlflow.set_tracking_uri()``` in the following code.<br>
# MAGIC > ![Link Azure ML Workspace](https://tsmatz.github.io/images/github/databricks/20200326_Link_AML.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following script and connect to your AML workspace.<br>
# MAGIC You should open https://microsoft.com/devicelogin and enter code to authenticate (login) in Azure.

# COMMAND ----------

# Run once in your cluster !
from azureml.core import Workspace
ws = Workspace.get(
  name = "<FILL-AML-WORKSPACE-NAME>",
  subscription_id = "<FILL-AZURE-SUBSCRIPTION-ID>",
  resource_group = "<FILL-RESOUCE-GROUP-NAME>")

# COMMAND ----------

# MAGIC %md
# MAGIC Set AML tracking URI in MLflow in order to redirect your MLflow logging into Azure Machine Learning.

# COMMAND ----------

import mlflow
tracking_uri = ws.get_mlflow_tracking_uri()
mlflow.set_tracking_uri(tracking_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC Set the expriment name for MLflow and AML workspace. (As you will see later, AML logs are saved in each experiments.)

# COMMAND ----------

experimentName = "databricks_mlflow_test"
mlflow.set_experiment(experimentName)

# COMMAND ----------

# MAGIC %md
# MAGIC Run Spark ML trainig jobs with MLflow logging.<br>
# MAGIC **All logs and metrics are transferred into Azure Machine Learning (Azure ML).**

# COMMAND ----------

# Read dataset
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv"))

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

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

# Drop rows with null values
df = df.dropna()

# Convert categorical values to indexer (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

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

# Define classifier
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol="features", labelCol="ARR_DEL15")

# Create pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])

# Prepare training with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
### 4 x 2 = 8 times training occurs (it takes a long time)
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 15, 20]) \
  .addGrid(classifier.maxBins, [251, 300]) \
  .build()
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction", metricName="weightedPrecision"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation

# Start mlflow and run pipeline
from mlflow import spark
mlflow.pyspark.ml.autolog() # (Remove here when you run on old version of ML runtime. See above.)
with mlflow.start_run() as parent_run:
  model = tvs.fit(df)                                               # logs above metric (weightedPrecision)
  mlflow.log_metric("bestPrecision", max(model.validationMetrics)); # logs user-defined custom metric
  mlflow.spark.log_model(model.bestModel, "model-file")             # logs model as artifacts

# COMMAND ----------

# MAGIC %md
# MAGIC Go to Azure Machine Learning workspace on [Azure Machine Learning Studio UI](https://ml.azure.com/) and click "Jobs" in the left-navigation.<br>
# MAGIC In a list of experiments, select a job named "databricks_mlflow_test" and select an experiment.<br>
# MAGIC You will see metrics ```bestPrecision```, a generated model, and other information. Run id in AML workspace is same as MLflow's run id.
# MAGIC
# MAGIC ![Azure ML Experiment UI](https://tsmatz.github.io/images/github/databricks/20200423_goto_experiment2.jpg)
# MAGIC
# MAGIC When you select "Child runs" tab, you can see details (```weightedPrecision```, etc) about 8 child runs.

# COMMAND ----------

# MAGIC %md
# MAGIC Show all run results with MLflow API.

# COMMAND ----------

current_experiment=mlflow.get_experiment_by_name(experimentName)
all_runs = mlflow.search_runs(
  experiment_ids=current_experiment.experiment_id)
all_runs

# COMMAND ----------

# MAGIC %md
# MAGIC This script extracts ```maxDepth```, ```maxBins```, and ```weightedPrecision``` in all child runs.

# COMMAND ----------

child_runs = mlflow.search_runs(
  filter_string="tags.mlflow.parentRunId = '{}'".format(parent_run.info.run_id))
child_runs = child_runs[[
  "params.DecisionTreeClassifier.maxDepth",
  "params.DecisionTreeClassifier.maxBins",
  "metrics.weightedPrecision"
]]
child_runs.rename(columns={
  "params.DecisionTreeClassifier.maxDepth": "maxDepth",
  "params.DecisionTreeClassifier.maxBins": "maxBins",
  "metrics.weightedPrecision": "weightedPrecision"}, inplace=True)
child_runs

# COMMAND ----------

# MAGIC %md
# MAGIC [Optional] You can also use AML API (AML Python SDK) to extract MLflow experiments as follows.

# COMMAND ----------

from azureml.core import Experiment
from azureml.core.run import Run
from mlflow.tracking import MlflowClient
import pandas as pd
 
# Use MlFlow to retrieve the child runs
client = MlflowClient()
ml_parent_run = Run(Experiment(ws, "databricks_mlflow_test"), '<FILL-RUN-ID>')
ml_child_runs = ml_parent_run.get_children()
l = []
for r in ml_child_runs:
  mlflow_child_run = MlflowClient().get_run(r.id)
  l.append([
    mlflow_child_run.data.params["DecisionTreeClassifier.maxDepth"],
    mlflow_child_run.data.params["DecisionTreeClassifier.maxBins"],
    mlflow_child_run.data.metrics["weightedPrecision"]
  ])
ml_df = pd.DataFrame(l, columns =["maxDepth", "maxBins", "weightedPrecision"])
display(ml_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deploy Model using Azure Machine Learning
# MAGIC
# MAGIC Now we deploy Spark ML pipeline model for serving (inferencing).
# MAGIC
# MAGIC You can generate a container image for serving with ```mlflow.azureml.build_image()``` and deploy this image in various platforms, such as, ACI, AKS, or edge devices.<br>
# MAGIC In this example, we deploy image on Azure Container Instance (ACI).
# MAGIC
# MAGIC > Note : With MLflow API, you can also load MLflow model as generic Python functions via ```mlflow.pyfunc.load_model()```.

# COMMAND ----------

# MAGIC %md
# MAGIC Save Spark ML pipeline model as MLflow model format.<br>
# MAGIC (Run Exercise 01 (Storage Settings) beforehand in order to mount ```/mnt/testblob```.)

# COMMAND ----------

# mlflow.spark.log_model(model.bestModel, "best-model")  # You can also log model in Azure Machine Learning
mlflow.spark.save_model(model.bestModel, "/mnt/testblob/flight_mlflow_model")

# COMMAND ----------

# MAGIC %md
# MAGIC Register (upload) model and create container image in Azure ML.

# COMMAND ----------

import mlflow.azureml
from azureml.core.webservice import AciWebservice, Webservice
registered_image, registered_model = mlflow.azureml.build_image(
  model_uri="/mnt/testblob/flight_mlflow_model",
  image_name="testimage",
  model_name="testmodel",
  workspace=ws,
  synchronous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the model serving in Azure Container Instance (ACI).

# COMMAND ----------

# create deployment config
aci_config = AciWebservice.deploy_configuration()
svc = Webservice.deploy_from_image(
  image=registered_image,
  deployment_config=aci_config,
  workspace=ws,
  name="testdeploy")
svc.wait_for_deployment(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC See details, if error has occured.

# COMMAND ----------

print(svc.get_logs())

# COMMAND ----------

# MAGIC %md
# MAGIC Show scoring (inference) Url.

# COMMAND ----------

svc.scoring_uri

# COMMAND ----------

# MAGIC %md
# MAGIC Now invoke scoring (inference) service !

# COMMAND ----------

import requests
import json
headers = {'Content-Type':'application/json'}
body = {
  "columns": [
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
    "DewPointCelsiusDest"
  ],
  "data": [
    [
      0,
      1,
      1,
      "DL",
      "ATL",
      "EWR",
      8,
      10,
      93.0,
      30.0175,
      12.05,
      7.5,
      1.0,
      11.025,
      96.5,
      30.055,
      4.5,
      6.0,
      0.5,
      3.95
    ],
    [
      0,
      1,
      1,
      "AA",
      "ABQ",
      "DFW",
      11,
      14,
      34.0,
      30.16,
      -2.8,
      15.0,
      10.0,
      -16.7,
      80.5,
      30.25,
      0.8,
      9.5,
      10.0,
      -2.1
    ]
  ]
}
http_res = requests.post(
  svc.scoring_uri,
  json.dumps(body),
  headers = headers)
print('Result : ', http_res.text)
