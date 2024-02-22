# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

#Files Names are Widgets
dbutils.widgets.text("File1","/mnt/adventureworks2022/Bronze/2024/30/01/Person.PersonPhone")
file1=dbutils.widgets.get("File1")

dbutils.widgets.text("File2","/mnt/adventureworks2022/Bronze/2024/29/01/Person.Person")
file2=dbutils.widgets.get("File2")

# COMMAND ----------

df1 = spark.read.format("parquet").option("inferschema", True).option("header", True).load(file1)
df2 = spark.read.format("parquet").option("inferschema", True).option("header", True).load(file2)

# COMMAND ----------

df1.show(10,False)
df2.show(10,False)

# COMMAND ----------

#Query
df_join = df2.join(df1, 'BusinessEntityID', 'inner').select(df2.BusinessEntityID, df2.FirstName, df2.LastName, df1.PhoneNumber).withColumn("PersistLastModifiedTime", lit(current_timestamp())).filter(df2.PersonType == 'IN')

#Date Format
from datetime import datetime
#date=datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

date=datetime.now().strftime("%Y_%m_%d")
year = datetime.now().strftime("%Y")  
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

#Write into ADLS
df_join.coalesce(1).write.option("header", "true").option("sep", ",").csv("/mnt/adventureworks2022/silver/" + year + "/" + month + "/" + day + "/" +"joined_tables.csv",mode="overwrite")

#Writing into Delta Table
df_join_delta=df_join.write.format("delta").saveAsTable("practicedb.joined_tables_"+date,mode="overwrite")
