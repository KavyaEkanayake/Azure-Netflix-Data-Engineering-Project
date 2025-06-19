# Databricks notebook source
# MAGIC %md
# MAGIC #  Incremental Data Loading using Auto loader 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new schema within the 'netflix_catalog'
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;

# COMMAND ----------

# Define the checkpoint location for Autoloader metadata and schema
checkpoint_location = "abfss://silver@netflixprojectdlkavya.dfs.core.windows.net/checkpoint"

# COMMAND ----------

# Configure the Autoloader to read streaming data from the 'raw' container
# It uses 'CloudFiles' format for Autoloader, specifies CSV as the file type,
# and points to the schema location for schema inference and evolution.
df = spark.readStream\
	.format("CloudFiles")\
	.option("CloudFiles.format", "csv")\
	.option("CloudFiles.SchemaLocation", checkpoint_location)\
	.load("abfss://raw@netflixprojectdlkavya.dfs.core.windows.net")

# COMMAND ----------

# Display the streaming DataFrame.
# 'display(df)' is a Spark action that initiates the streaming query and shows a live preview of the data as it's being ingested and processed. It's often used for interactive development
# and debugging of streaming pipelines.
display(df)

# COMMAND ----------

# Configure the write stream to continuously write processed data to the 'bronze' container.
# It uses the checkpoint location for exactly-once processing guarantees.
# The 'processingTime' trigger ensures data is processed every 10 seconds.
df.writeStream\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(processingTime='10 seconds')\
  .start("abfss://bronze@netflixprojectdlkavya.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

