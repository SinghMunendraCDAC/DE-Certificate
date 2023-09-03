# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 13 : Databricks SQL
# MAGIC
# MAGIC In Exercise 11, we have seen data warehouse (Azure Synapse Analytics) integration with Databricks to make data available for end users, such as, decision makers or business persons.<br>
# MAGIC By using Databricks SQL, you can directly expose data in object storage on Azure Databricks.
# MAGIC
# MAGIC As I have explained in Exercise 9, delta lake can provide ACID isolation (such like, data warehouse) and you can then manage every kinds of data - such as, batch processing data, streaming data, or business intelligence data - in a consistent lakehouse storage.
# MAGIC
# MAGIC With Databricks SQL, you can connect data from external applications, such as, Power BI, Tableau, dbt, etc.
# MAGIC
# MAGIC ![Databricks SQL architecture](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Architecture.jpg)
# MAGIC
# MAGIC But, in this exercise, we'll create a dashboard and visualize data within Azure Databricks.
# MAGIC
# MAGIC > Note : Databricks SQL requires the Premium plan.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data
# MAGIC
# MAGIC In this exercise, we also use ```flight_weather.csv ```. Before starting, run "Exercise 01 : Storage Settings".

# COMMAND ----------

# MAGIC %md
# MAGIC Next we create a Spark table by running the following sql script. (Replace the following placeholder.)
# MAGIC
# MAGIC > Note : Here we use CSV file, but delta lake is recommended to use. (See Exercise 9.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_details
# MAGIC USING CSV
# MAGIC OPTIONS (header "true", nullValue "NA", "inferSchema" TRUE)
# MAGIC LOCATION "abfss://<FILL-CONTAINER-NAME>@<FILL-STORAGE-ACCOUNT-NAME>.dfs.core.windows.net/flight_weather.csv";

# COMMAND ----------

# MAGIC %md
# MAGIC Check whether a table is correctly created.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flight_details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SQL Warehouse
# MAGIC
# MAGIC Databricks SQL warehouse is a computing resource for Databricks SQL (such like, Databricks runtime in other exercises).

# COMMAND ----------

# MAGIC %md
# MAGIC First of all, we should start SQL warehouse in Azure Databricks.<br>
# MAGIC Please go to "SQL" in the top-left navigation. (See below.)
# MAGIC
# MAGIC ![Start Databricks SQL](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Start.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Select "SQL Warehouses" menu, and click "Create SQL Warehouse" to start a new warehouse.<br>
# MAGIC In the "New SQL Warehouse" wizard (see below), please set appropriate values and push "Create" button.
# MAGIC
# MAGIC ![Start Databricks SQL](https://tsmatz.github.io/images/github/databricks/20221215_Sql_New_Warehouse.jpg)
# MAGIC
# MAGIC > Note : In this example, we have created a dedicated warehouse, but you can also use a **serverless SQL warehouse**, in which the compute is consumed only on running queries. When the workload will be idle (inactive) in most cases and the query sometimes happens by ad-hoc, serverless SQL warehouses will be the best choice for your workload. However, when the queries will always arrive in use, it will be better to select a dedicated warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC When you select "Data" menu, you will see ```filght_details``` table as follows.
# MAGIC
# MAGIC ![Data](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Data.jpg)
# MAGIC
# MAGIC > Note : You can also ingest and upload a new table in this "Data" pane.

# COMMAND ----------

# MAGIC %md
# MAGIC To enable SQL warehouse accessing your storage, you should set AAD service principal in SQL warehouse by the following steps.
# MAGIC
# MAGIC - Create AAD service principal and assign "**Storage Blob Data Contributor**" role in storage account.<br>
# MAGIC   To do this, follow the steps of "Approach 2" in Exercise01.
# MAGIC - Set service principal's secret (key) in Azure Key Vault, and create a secret scope in Azure Databricks.<br>
# MAGIC   To do this, follow the steps of "Approach 3" in Exercise01.
# MAGIC - Click the top-right user's name and select "SQL Admin Console" (Admin Settings).<br>
# MAGIC   ![Add service principal](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Admin_Console.jpg)
# MAGIC - Click "SQL Warehouse settings" tab and click "Add service principal" button.<br>
# MAGIC   In the pop-up window, please fill the information for the service principal as follows and push "Add" button.<br>
# MAGIC   ![Add service principal](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Service_Principal.jpg)
# MAGIC - Make sure that the following will then be added in the SQL warehouse's properties. (You can also directly edit and change these properties.)
# MAGIC
# MAGIC ```
# MAGIC spark.hadoop.fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net OAuth
# MAGIC spark.hadoop.fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC spark.hadoop.fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net <service-principal-client-id>
# MAGIC spark.hadoop.fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net {{secrets/<secret-scope-name>/<secret-key-name>}}
# MAGIC spark.hadoop.fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net https://login.microsoftonline.com/<tenant-id>/oauth2/token
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Go back to "Data" menu again.<br>
# MAGIC When you select ```flight_details``` and click "Sample Data" tab, you can then see the data in this table as follows.
# MAGIC
# MAGIC ![View data](https://tsmatz.github.io/images/github/databricks/20221215_Sql_View_Data.jpg)
# MAGIC
# MAGIC Now your data is ready in SQL warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Query

# COMMAND ----------

# MAGIC %md
# MAGIC In order to add a query, please select "Queries" menu in navigation and push "Create query" button.
# MAGIC
# MAGIC In SQL window, fill the following text.
# MAGIC
# MAGIC ```
# MAGIC USE default;
# MAGIC
# MAGIC SELECT
# MAGIC   T.DAY_OF_WEEK,
# MAGIC   T.AIR_TIME, 
# MAGIC   T.DISTANCE
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       flight_details
# MAGIC     WHERE
# MAGIC       DEST in ({{ pickup_dest }})
# MAGIC   ) T
# MAGIC ORDER BY
# MAGIC   T.YEAR, T.MONTH, T.DAY_OF_MONTH
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC This will automatically shows you ```pickup_dest``` widget in query UI.<br>
# MAGIC Fill "JFK" in this widget and click "Apply changes". It will then run this query and show the query results as follows.
# MAGIC
# MAGIC ![Query](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Query.jpg)
# MAGIC
# MAGIC Make sure to click "Save" button, and fill the query name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dashboard and Visualize

# COMMAND ----------

# MAGIC %md
# MAGIC Select "Dashboards" menu and click "Create dashboard".<br>
# MAGIC Enter the name of dashboard and proceed to create a new dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC Select "Add" - "Visualization" in order to insert a new visualization in this dashboard.
# MAGIC
# MAGIC ![Add new visualization](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Visual_Add.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC In the creation wizard, please select the query which you have just created above, and proceed to the next.
# MAGIC
# MAGIC ![Select query](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Select_Query.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC In the wizard, please set the following styles as follows, and press "Save" button.
# MAGIC
# MAGIC - Visualization type : Scatter
# MAGIC - X column : ```DISTANCE```
# MAGIC - Y columns : ```AIR_TIME```
# MAGIC - Group by : ```DAY_OF_WEEK```
# MAGIC
# MAGIC ![Set visualiztion style](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Visualization_Styles.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC This dashboard will show you the air time in the flights which is color-coded by the day of week.<br>
# MAGIC You can also change the destination airport in the widget on this screen.
# MAGIC
# MAGIC ![Generated dashboard](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Dashboard.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop SQL warehouse
# MAGIC
# MAGIC After completion, please stop SQL warehouse not to waste computing consumption.
# MAGIC
# MAGIC ![Stop SQL warehouse](https://tsmatz.github.io/images/github/databricks/20221215_Sql_Stop.jpg)
