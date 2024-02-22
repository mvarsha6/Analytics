# Databricks notebook source
#set the data lake file location:
file_location = "/mnt/databrics/GoldStock/goldstock.csv"
 
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#display the dataframe
display(df)

# COMMAND ----------

#The data is directly being loaded as a Data Frame
type(df)

# COMMAND ----------

df.show(10,False)

# COMMAND ----------

df=df.withColumnRenamed("_c0","ID")

# COMMAND ----------

df.show(10,False)

# COMMAND ----------

type(df["Date"])
from pyspark.sql.functions import *
new_df=df.withColumn("year",lit(year('Date')))
new_df.show(5,False)

# COMMAND ----------

#Adding a new Column 
new_df = new_df.withColumn("Month-year", lit(concat(month("Date"), lit("-"), "year")))

new_df.show(5,False)

# COMMAND ----------

#Creating a View on a DataFrame, we can execute SQL Queries on this
new_df.createOrReplaceTempView("new_df2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM new_df2

# COMMAND ----------

#Performing union and union all on dataframes

#Creating new Dataframe
file_location = "/mnt/databrics/GoldStock/goldstock.csv" 
df2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)

#Performing Union
union_df=df.union(df2)

#Performing Union all
union_all_df=df.unionAll(df2)

# COMMAND ----------

#performing union on select columns
df1_top10 = df.limit(10)
df1_selected=df1_top10.select("ID","Close","Volume")  #--> Taking first 3 Columns

df2_top10 = df2.limit(10)
df2_selected=df2_top10.select("Open","High","Low")    #--> Taking last 3 Columns

union_all_df2 = df1_selected.unionAll(df2_selected)

#Displaying the results
union_all_df2.show()


# COMMAND ----------

#Write Data in ADLS
#union_all_df2.write.csv("/mnt/databrics/Output/goldstock_output1.csv")
