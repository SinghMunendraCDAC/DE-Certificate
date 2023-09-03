# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 11 : Orchestration with Azure Data Services
# MAGIC
# MAGIC In this exercise, we create ETL (Extract, Transform, and Load) flows with Azure Databricks and a variety of Azure Data services.<br>
# MAGIC There will be the following 2 main options for orchestration :
# MAGIC
# MAGIC - Orchestration with **Azure Data Factory (ADF)**. You can invoke a pipeline in ADF by scheduling time-based triggers or manual triggers (through REST API, SDK, etc).
# MAGIC - Orchestrate jobs in Databricks with **Databricks Workflows**. You can also include the incremental data loading with Delta Live Tables (see Exercise 12) in Databricks Workflows.<br>
# MAGIC Using Databricks, you can also integrate multiple notebooks and build notebook's workflows. (Notebook can also handle both inputs and outputs.)
# MAGIC
# MAGIC In this exercise, we'll create an orchestration pipeline in Azure Data Factory (ADF).<br>
# MAGIC In this pipeline,
# MAGIC
# MAGIC - Extract (ingest) data from public web site. (In this exercise, we use a Blob SAS URL instead of using public web site.)
# MAGIC - Transform data on Databricks with distributed manners.
# MAGIC - Load data into data warehouse, Azure Synapse Analytics.
# MAGIC
# MAGIC End users can then query large aggregated data with BI tools, such as, Power BI or Tableau.
# MAGIC
# MAGIC ![Pipeline Diagram](https://tsmatz.github.io/images/github/databricks/20191114_Pipeline_Diagram.jpg)
# MAGIC
# MAGIC > Note : Using **Mapping Data Flows** in Azure Data Factory, You can also build data transformation on Apache Spark with visual UI.
# MAGIC
# MAGIC > Note : Using **Databricks SQL** (lakehouse architecture), end users can also query data in data lake from BI tools (such as, Power BI or Tableau) without ingesting data into data warehouse.<br>
# MAGIC > See Exercise 13.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare resource for Azure Data Factory (ADF)
# MAGIC
# MAGIC 1. Go to [Azure Portal](https://portal.azure.com) and create a new Data Factory (ADF) resource.
# MAGIC     - Select "V2" for Data Factory version in creation wizard.
# MAGIC 2. After a ADF resource is created, go to resource blade and open Azure Data Factory Studio by clicking "Author & Monitor". (See below.)<br>
# MAGIC
# MAGIC ![ADF Blade](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Blade.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract (Copy) Flight Dataset
# MAGIC
# MAGIC Now we create a pipeline (ADF pipeline) to copy dataset from source to destination.<br>
# MAGIC Here we use US flight dataset of year 2008 (originated by [BTS (Bureau of Transportation Statistics in US)](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time), approximately 700 MB, 7,000,000 rows).
# MAGIC
# MAGIC 1. Prepare source file :<br>
# MAGIC Instead of using public web site, here I prepare URL to access data in Azure storage.
# MAGIC     - Before starting, create an Azure storage account resource and create a container in blob storage.
# MAGIC     - Please download ```2008.csv.bz2``` (US airline on-time dataset 2008) from [here](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7), and put this file on your blob conatiner.<br>
# MAGIC     - Using a blade on Azure blob storage in Auzre Portal, create shared access token (SAS token). (See below.)<br>
# MAGIC     The URL to access ```2008.csv.bz2``` will be : ```https://{storage name}.blob.core.windows.net/{container name}/2008.csv.bz2?{SAS token}```<br>
# MAGIC     ![Create SAS token](https://tsmatz.github.io/images/github/databricks/20220624_sas_token.jpg)
# MAGIC 2. Prepare destination location :<br>
# MAGIC Create an Azure Data Lake Storage Gen2 resource (create a storage account and enable "Enable hierarchical namespace" checkbox in creation wizard).<br>
# MAGIC After the storage is created,
# MAGIC     - Create a container in blob storage.
# MAGIC     - Create a sub directory for saving file
# MAGIC 3. Create a pipeline :<br>
# MAGIC In ADF Studio UI, start to create pipeline.    
# MAGIC ![Create Pipeline](https://tsmatz.github.io/images/github/databricks/20190207_Create_Pipeline.jpg)
# MAGIC 4. Create a linked service for source :<br>
# MAGIC In pipeline editor, create new connection. (As the following picture shows, click "Connections" and "New".)<br>
# MAGIC ![Create Connection](https://tsmatz.github.io/images/github/databricks/20190207_Create_Connections.jpg)<br>
# MAGIC In the next wizard, select "HTTP" for data store.<br>
# MAGIC ![Select HTTP](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_HTTP.jpg)<br>
# MAGIC In the next wizard, set the following properties and finish.
# MAGIC     - Base URL : ```https://{storage name}.blob.core.windows.net/{container name}/2008.csv.bz2?{SAS token}``` (above URL)
# MAGIC     - Authentication Type : Anonymous
# MAGIC 5. Create a linked service for destination :<br>
# MAGIC In pipeline editor, create new connection again and select "Azure Data Lake Storage Gen2" for platform.<br>
# MAGIC ![Select ADLS Gen2](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_ADLSGen2.jpg)<br>
# MAGIC In the next wizard, select "Managed Service Identity (MSI)" (or System-assigned managed identity) in "Authentication method" property and copy the generated identity application ID (client ID) into the clipboard. (See below.)<br>
# MAGIC ![Authentication Setting for ADLS Gen2](https://tsmatz.github.io/images/github/databricks/20190207_ADF_ADLSAuth.jpg)
# MAGIC 6. Set access permission in Data Lake Storage Gen2 :<br>
# MAGIC Go to Azure Data Lake Storage Gen 2 resource in Azure Portal and click "Access Control (IAM)" on navigation.
# MAGIC     - Click "Add role assignment"
# MAGIC     - Set as follows and save
# MAGIC         - Role : Storage Blob Data Contributor
# MAGIC         - Member : (previously copied ADF managed identity)
# MAGIC 7. Add copy activity in a pipeline :<br>
# MAGIC In pipeline editor, drag and insert "Copy Data" activity in your pipeline.    
# MAGIC ![Insert Copy Activity](https://tsmatz.github.io/images/github/databricks/20190207_Copy_Activity.jpg)
# MAGIC 8. Set source dataset :<br>
# MAGIC Select "Source" tab and click "New". (See below.)    
# MAGIC ![Soutce Setting](https://tsmatz.github.io/images/github/databricks/20190207_New_Datasource.jpg)
# MAGIC     - In the wizard, select "HTTP" for data source type
# MAGIC     - In dataset setting, select "Connection" tab and fill the following properties.
# MAGIC         - Linked service : (select previously created "HTTP" connection)
# MAGIC         - Relative Url : (blank)
# MAGIC         - Request Method : GET
# MAGIC         - Compression type : BZip2
# MAGIC         - Compression level : Optimal
# MAGIC         - First row as header : True<br>
# MAGIC         ![Dataset Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_FlightData.jpg)
# MAGIC 9. Set destination dataset :<br>
# MAGIC Select "Sink" tab in the activity setting and create new destination dataset (click "New").    
# MAGIC ![Sink Setting](https://tsmatz.github.io/images/github/databricks/20190207_New_Datasink.jpg)
# MAGIC     - In the wizard, select "Azure Data Lake Storage Gen2" for data source type
# MAGIC     - In dataset setting, select "Connection" tab and fill the following properties
# MAGIC         - Linked service : (select previously created "Azure Data Lake Storage Gen2" connection)
# MAGIC         - File Path : (a sub directory in your ADLS Gen2. see above)
# MAGIC         - First row as header : True
# MAGIC         - File extension : .txt
# MAGIC 10. Publish pipeline :<br>
# MAGIC Finally, publish entire pipeline. (If you're linking your own repository, json file will be saved as pipeline definitions into your repository.)
# MAGIC
# MAGIC You can easily see whether your settings are successfully configured by debugging. (Click "Debug" button.)<br>
# MAGIC When it succeeded, the flight data is copied as CSV (.txt file) into the directory in Azure Data Lake Storage Gen2.
# MAGIC
# MAGIC ![ADF Debug](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Debug.jpg)
# MAGIC
# MAGIC > Note : In production, you can also pass the parameters (such as file location, etc) into your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare a resource for Azure Databricks
# MAGIC
# MAGIC 1. Go to cluster settings on Azure Databricks (go to "compute" and select a cluster) and click "Edit".
# MAGIC 2. Set storage access key for Azure Data Lake Storage (Gen2) in Spark config. (Cluster restart is required.)<br>
# MAGIC Follow "[Exercise 01 : Storage Settings](https://tsmatz.github.io/azure-databricks-exercise/exercise01-blob.html)" for details.
# MAGIC 3. Open "User Settings" UI and generate access token by clicking "Generate New Token" button. (See below.)<br>
# MAGIC Copy the generated token into the clipboard.<br>
# MAGIC ![Generate Databricks Token](https://tsmatz.github.io/images/github/databricks/20190207_Databricks_Token.jpg)
# MAGIC 4. Open [Exercise11/simple-transform-notebook](https://tsmatz.github.io/azure-databricks-exercise/Exercise11/simple-transform-notebook.html) in Databricks workspace and change placeholders in the fist cell and last cell. (Set storage account name, container name, and sub directory name in Azure Data Lake Storage Gen2, which is previously created.)
# MAGIC
# MAGIC ```
# MAGIC df = (sqlContext.read.format("csv").
# MAGIC   option("header", "true").
# MAGIC   option("nullValue", "NA").
# MAGIC   option("inferSchema", True).
# MAGIC   load("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/<FILL-SUB-DIRECTORY-NAME>/*"))
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC df.write.parquet("abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flights_parquet", mode="overwrite")
# MAGIC ```
# MAGIC
# MAGIC > Note : In production, you can pass these variables as parameters in notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Dataset on Azure Databricks
# MAGIC
# MAGIC Here we insert Databricks' notebook activity in ADF pipeline to consume data (CSV) in this notebook.    
# MAGIC Using Azure Data Lake Storage as a common data store, the data is not transferred (not moved) across each activities.
# MAGIC
# MAGIC 1. Open Azure Data Factory Studio UI and create new connection. (Click "Connections" and "New" button.)    
# MAGIC In the wizard, click "Compute" tab and select "Azure Databricks" from the list of connection type.    
# MAGIC ![Select Azure Databricks](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_Databricks.jpg)    
# MAGIC In the connection setting, fill the following properties and finish. (See note in below.)
# MAGIC     - Databricks workspace : (select your Databricks resource in Azure)
# MAGIC     - Select cluster : Existing interactive cluster
# MAGIC     - Access token : (previously copied access token in Azure Databricks)
# MAGIC     - Choose from existing cluster : (select your interactive cluster)    
# MAGIC     ![Databricks Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Databricks.jpg)
# MAGIC 2. Drag notebook activity into your pipeline and connect with the previous "Copy Data" activity.    
# MAGIC ![Insert Notebook Activity](https://tsmatz.github.io/images/github/databricks/20190207_Notebook_Activity.jpg)
# MAGIC     - Click "Azure Databricks" tab in this activity setting and select above connection as Databricks linked service.    
# MAGIC       Click "Edit" and set properties again.
# MAGIC     - Next select "Settings" tab. Click "Browse" button in "Notebook path" section and select ```Exercise11/simple-transform-notebook``` in this hands-on.<br>
# MAGIC     ![Databricks Dataset Setting](https://tsmatz.github.io/images/github/databricks/20190207_Databricks_Dataset.jpg)
# MAGIC 3. Publish the entire pipeline again.    
# MAGIC
# MAGIC After you have completed, please click "Debug" button in pipeline and see if the parquet (```/flights_parquet```) is generated in Data Lake storage container.
# MAGIC
# MAGIC > Note : In this example, I have used Databricks **interactive cluster (all-purpose compute)** for debugging purpose, but you can use the cost saving **job cluster (job compute)** in production.<br>
# MAGIC > The cluster will be automatically turned on and then terminated, when it's inactive.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare resource for Azure Synapse Analytics (formerly, SQL DW)
# MAGIC
# MAGIC 1. Create a new "Azure Synapse Analytics" (formerly, Azure SQL Data Warehouse) resource in [Azure Portal](https://portal.azure.com).<br>
# MAGIC After the resource is created, create a dedicated SQL pool.<br>
# MAGIC ![New SQL DW](https://tsmatz.github.io/images/github/databricks/20190207_New_Sqldw.jpg)
# MAGIC 2. Open Synapse Studio UI and develop tab.
# MAGIC 3. Create SQL script and connect to the previous dedicated SQL pool.<br>
# MAGIC In script editor, run the following query to create a table.
# MAGIC
# MAGIC ```
# MAGIC CREATE TABLE flightdat_dim(
# MAGIC [Year] [int] NULL,
# MAGIC [Month] [int] NULL,
# MAGIC [DayofMonth] [int] NULL,
# MAGIC [DayOfWeek] [int] NULL,
# MAGIC [UniqueCarrier] [varchar](50) NULL,
# MAGIC [Origin] [varchar](50) NULL,
# MAGIC [Dest] [varchar](50) NULL,
# MAGIC [CRSDepTime] [int] NULL,
# MAGIC [CRSArrTime] [int] NULL,
# MAGIC [DepDelay] [int] NULL,
# MAGIC [ArrDelay] [int] NULL,
# MAGIC [CancelledFlag] [bit] NULL,
# MAGIC [DivertedFlag] [bit] NULL,
# MAGIC [ArrDelay15Flag] [bit] NULL);
# MAGIC
# MAGIC CREATE STATISTICS flightdat_stat on flightdat_dim(Year, Month, DayofMonth);
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data into Azure Synapse Analytics
# MAGIC Once data (parquet) is generated, you can efficiently load (import) data into Azure Synapse Analytics with bulk data loader (such as, PolyBase) by using any of the following 3 ways :
# MAGIC
# MAGIC - Using T-SQL directly
# MAGIC - Using Azure Data Factory connector for Azure Synapse Analytics
# MAGIC - Using Azure Databricks connector for Azure Synapse Analytics
# MAGIC
# MAGIC In this exercise, we use ADF connector to load data into Azure Synapse Analytics (dedicated SQL pool).
# MAGIC
# MAGIC 1. Go to pipeline editor on ADF Studio UI and create new connection. (Click "Connections" and "New" button.)    
# MAGIC In the wizard, select "Azure Synapse Analytics" from the list of connections.    
# MAGIC ![Select Azure SQL Data Warehouse](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_SQLDW.jpg)    
# MAGIC In connection setting, fill the following properties.
# MAGIC     - server name : (your db server name)
# MAGIC     - database name : (your database name)
# MAGIC     - Authentication type : SQL Authentication
# MAGIC     - User name : (server user name)
# MAGIC     - Password : (password)    
# MAGIC     ![SQL DW Connection Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_SQLDW.jpg)
# MAGIC 2. Drag "Copy Data" activity into the pipeline and connect with notebook activity. (See below.)    
# MAGIC ![Copy Activity](https://tsmatz.github.io/images/github/databricks/20190207_Copy_Activity2.jpg)
# MAGIC 3. In the activity setting, click "Source" tab and create new source dataset (click "New")
# MAGIC     - In the wizard, select "Azure Data Lake Storage Gen2" from the list of dataset type
# MAGIC     - In dataset setting, select "Connection" tab and fill as follows
# MAGIC         - Linked service : (previously generated ADLS Gen2 connection)
# MAGIC         - File Path - Directory : (your container name/flights_parquet)
# MAGIC         - File Path - File : part-*
# MAGIC         - File Format : Parquet format    
# MAGIC         ![Source Setting](https://tsmatz.github.io/images/github/databricks/20190207_Parquet_Source.jpg)
# MAGIC 4. In the activity setting, click "Sink" tab and create new destination dataset (click "New")
# MAGIC     - In the wizard, select "Azure Synapse Analytics" from the list of dataset type
# MAGIC     - In dataset setting, click "Edit", select "Connection" tab, and fill as follows
# MAGIC         - Linked service : (previously generated Synapse Analytics connection)
# MAGIC         - Table : [dbo].[flightdat_dim]
# MAGIC 5. **Warning : Currently (Feb 2019) ADF doesn't support ADLS Gen2 storage (with hierarchical namespace) for Synapse Analytics ingestion by PolyBase. For this reason, you need interim staging account.**    
# MAGIC Please proceed the following extra steps.
# MAGIC     - Create another storage account (not Gen2) resource in Azure Portal.
# MAGIC     - Create a new connection for "Azure Blob Storage" in ADF pipeline GUI editor.
# MAGIC     - In this activity setting ("Copy Data" activity for Synapse Analytics), click "Settings" tab, turn on "Enable staging", and select generated blob connection for staging account.
# MAGIC
# MAGIC > Note : Here we simply uses flat data format, but the complex data type (nested type) is not supported for both Azure Synapse Analytics and ADF. Please convert to flat schema before data loading.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Entire Pipeline
# MAGIC
# MAGIC In pipeline editor, click "Debug" and run entire pipeline.
# MAGIC
# MAGIC ![Debug and Run](https://tsmatz.github.io/images/github/databricks/20190207_Run_Result.jpg)
# MAGIC
# MAGIC After the pipeline has succeeded, see the data in Azure Synapse Analytics with the following SQL query.
# MAGIC
# MAGIC ```select * from flightdat_dim where Year = 2008 and Month = 5 and DayOfMonth = 25```
# MAGIC
# MAGIC ![Query Result](https://tsmatz.github.io/images/github/databricks/20190207_SQL_Result.jpg)
# MAGIC
# MAGIC When your experimentation is completed, please stop (pause) Azure Synapse Analytics for saving money. (Click "Pause" in Synapse Analytics resource in Azure Portal.)
