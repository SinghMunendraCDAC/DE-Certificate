# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 01 : Storage Settings (Prepare)
# MAGIC
# MAGIC Databricks is well-architected by separating artifacts (files, models, etc) from compute clusters.<br>
# MAGIC By this design of architecture, you are free to remove or change your cluster without losing any artifacts. (The artifacts will remain even when you remove your computing cluster.)
# MAGIC
# MAGIC ![Databricks architecture](https://tsmatz.github.io/images/github/databricks/20220201_databricks_architecture.jpg)
# MAGIC
# MAGIC In this first exercise, we prepare Azure storage and then connect to your Databricks workspace.<br>
# MAGIC In the following all exercises, we will use this provisioned storage, and then make sure to do this exercise before running other notebooks.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation (Common)
# MAGIC
# MAGIC Before you start, please create a resource for **Azure Data Lake Storage Gen2** storage in [Microsoft Azure](https://portal.azure.com/). :
# MAGIC
# MAGIC 1. Create a Storage Account resource in [Azure Portal](https://portal.azure.com/).<br>
# MAGIC In the creation wizard, enable "**Enable hierarchical namespace**" checkbox in "Advanced" setting.
# MAGIC 2. Create a container (file system) in blob storage as follows.
# MAGIC   - Select "Containers" in the left navigation on Storage Account resource.
# MAGIC   - Add a container (private container).
# MAGIC 3. Upload [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) on the root in this container.<br>
# MAGIC (You can also use Azure Storage Explorer for storage operations.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 1 : Hadoop filesystem extension
# MAGIC
# MAGIC (Required for Exercise 03, 04, 05, 10, 13)
# MAGIC
# MAGIC You can access objects (files) in Databricks with a filesystem extension, such as "abfss://" (Azure Data Lake Storage Gen 2) or "wasbs://" (Azure blob storage).
# MAGIC
# MAGIC Before starting,
# MAGIC
# MAGIC 1. Copy **access key** in your storage account. (Click "Access Keys" in storage account resource.)
# MAGIC 2. Fill the above access key in Spark Config as follows.
# MAGIC   - Click "Compute" and go to your cluster setting.
# MAGIC   - Click "Edit" button to modify setting.
# MAGIC   - Click "Advanced options" section in cluster setting.
# MAGIC   - Fill the following text in "Spark config".<br>
# MAGIC     ```fs.azure.account.key.YOUR_STORAGE_ACCOUNT_NAME.dfs.core.windows.net YOUR_STORAGE_ACCOUNT_KEY```<br>
# MAGIC     For instance,<br>
# MAGIC     ```fs.azure.account.key.demostore01.dfs.core.windows.net Fbur3ioIw+...```
# MAGIC 3. Restart your cluster
# MAGIC
# MAGIC > Note : It will show you alerts not to hard code access key, but please ignore if it's for temporal purpose. In production, please use the following Databricks scope (approach 3).

# COMMAND ----------

# MAGIC %md
# MAGIC Access ```flight_weather.csv``` in Azure storage. (Change the following placeholder before running.)

# COMMAND ----------

df = sqlContext.read.format("csv").\
  option("header", "true").\
  option("nullValue", "NA").\
  option("inferSchema", True).\
  load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv")
df.cache()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 2 : Mount with service principal (secret)
# MAGIC
# MAGIC (Required for Exercise 03, 06, 10, 13)
# MAGIC
# MAGIC You can mount Azure blob container and access with ordinary UNIX path expression (such as, ```/mnt/tmp```).
# MAGIC
# MAGIC Before starting, create a service principal in Azure and set permissions as follows.
# MAGIC
# MAGIC 1. Register new application (service principal) in your Azure Active Directory.<br>
# MAGIC (Select "Azure Active Directory" menu in Azure Portal and select "App registrations". In app registration pane, register a new app.)
# MAGIC 2. Generate new client secret (application's password) in this service principal.<br>
# MAGIC (You can create a secret from "certificates and secrets" menu.)
# MAGIC 3. In your storage account, assign "**Storage Blob Data Contributor**" role for this service principal.<br>
# MAGIC (Select "Access control (IAM)" menu in your storage account and add new role assignment.)
# MAGIC 4. Copy application id (client id) and secret (key) in this service principal.<br>
# MAGIC These values are used in the following exercise.
# MAGIC
# MAGIC > Note : Service principal is an application's identity used for accessing to the backend Azure resources. See "[Use Azure REST API without interactive Login UI with app key or certificate programmatically](https://tsmatz.wordpress.com/2017/03/03/azure-rest-api-with-certificate-and-service-principal-by-silent-backend-daemon-service/)" for details.

# COMMAND ----------

# MAGIC %md
# MAGIC Mount your Azure storage with Databricks utilities as follows. (Change the following placeholder before running.)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/",
  mount_point = "/mnt/testblob",
  extra_configs = {"fs.azure.account.auth.type": "OAuth",
                   "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                   "fs.azure.account.oauth2.client.id": "<FILL-CLIENT-ID>",
                   "fs.azure.account.oauth2.client.secret": "<FILL-CLIENT-SECRET>",
                   "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<FILL-YOUR-DIRECTORY-DOMAIN-OR-TENANT-ID>/oauth2/token"})

# COMMAND ----------

# MAGIC %md
# MAGIC To access the mounted folder with Databricks File System (shortly, DBFS), use dbfs utilities as follows.

# COMMAND ----------

dbutils.fs.ls("/mnt/testblob")

# COMMAND ----------

# MAGIC %md
# MAGIC To access mounted folder with regular shell commands, run as follows.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/testblob

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 3 : [Optional] Use Azure Key Vault and Databricks Scope
# MAGIC
# MAGIC (Required for Exercise 13 only)
# MAGIC
# MAGIC By using Databricks scope with Azure Key Vault, you can access your storage without hard-coding secure information (such as, access key, secrets, etc).
# MAGIC
# MAGIC Before starting,
# MAGIC 1. Create a key vault resource in [Azure Portal](https://portal.azure.com/).
# MAGIC 2. Create new secret in key vault and set service principal's secret (see above) as a value. Here we assume the secret name is "testsecret01".
# MAGIC 3. Go to https://YOUR_AZURE_DATABRICKS_URL#secrets/createScope    
# MAGIC (Once you've created scope, you can manage scope with Databricks CLI.)    
# MAGIC ![Secret Scope](https://tsmatz.github.io/images/github/databricks/20191225_Create_Scope.jpg)
# MAGIC 4. In this setting,
# MAGIC     - Fill arbitrary "Scope Name". Here we assume the name is "scope01", which is needed for the following "dbutils" commands.
# MAGIC     - Select "Creator" in "Manage Principal" (needing Azure Databricks Premium tier).
# MAGIC     - Fill your key vault's setting for "DNS Name" and "Resource ID". (You can copy settings in key vault's "Properties".)
# MAGIC
# MAGIC > Note : You can also use Databricks-backed secret scope (built-in scope) instead of using Azure Key Vault.
# MAGIC
# MAGIC > Note : You create a secret scope using the Databricks CLI (version 0.7.1 and above) or Secrets API 2.0 instead of using UI.

# COMMAND ----------

# MAGIC %md
# MAGIC Mount your Azure storage with Databricks scope as follows. (Change the following placeholder before running.)

# COMMAND ----------

sp_secret = dbutils.secrets.get(scope = "scope01", key = "testsecret01")
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<FILL-CLIENT-ID>",
           "fs.azure.account.oauth2.client.secret": sp_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<FILL-YOUR-DIRECTORY-DOMAIN-OR-TENANT-ID>/oauth2/token"}
dbutils.fs.mount(
  source = "abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net",
  mount_point = "/mnt/testblob02",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC Access to the mounted folder.

# COMMAND ----------

dbutils.fs.ls("/mnt/testblob02")

# COMMAND ----------

# MAGIC %md
# MAGIC Unmount after you have completed.

# COMMAND ----------

dbutils.fs.unmount("/mnt/testblob02")
