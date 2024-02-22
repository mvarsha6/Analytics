# Databricks notebook source
# MAGIC %sql
# MAGIC Create DATABASE PracticeDB

# COMMAND ----------

#set the data lake file location:
file_location2 = "/mnt/databrics/Goldtock/goldstock.csv"
 
#read in the data to dataframe df
df2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location2)
 
#display the dataframe
df2.show(5,False)

# COMMAND ----------

#Writing Dat into Delta Tables

df2_delta=df2.write.format("delta").saveAsTable("practicedb.goldstock")

# COMMAND ----------

# Write Data from delta tables into ADLS

# Read in the data to dataframe df
delta_table = spark.read.format("delta").load("dbfs:/user/hive/warehouse/practicedb.db/employee")

# Write to ADLS using DeltaTable
delta_table.write.format("delta").save("/mnt/databrics/Output/goldstock_delta")
