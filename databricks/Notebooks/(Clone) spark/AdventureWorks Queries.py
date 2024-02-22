# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://adventureworks2022@adlsdwdev.blob.core.windows.net/",
    mount_point='/mnt/adventureworks2022',
    extra_configs={
        "fs.azure.account.key.adlsdwdev.blob.core.windows.net": 
            'sbdV10I9xQ/icSc17/AI0WCRBpO4AHLXHBJjmTJ3B4PEzXNUkzFvOispc36+cmT9KqOthBeErl9C+ASt9gUrAw=='            
    }
)

# COMMAND ----------

#Query-1
#set the data lake file location:
file_location1 = "/mnt/adventureworks2022/Production/ProductionProductInventory"
 
#read the data to dataframe df
df1 = spark.read.format("parquet").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location1)

#Query
from pyspark.sql.functions import *

req_cols = df1.select('ProductID', 'Quantity', 'Shelf').filter((df1.Shelf == 'A') | (df1.Shelf == 'C') | (df1.Shelf == 'H'))
req_cols=req_cols.groupBy('ProductID').agg(sum('Quantity').alias("TotalQuantity")).filter(sum('Quantity')>500).orderBy('ProductID')

req_cols=req_cols.withColumn('PersistLastModifiedTime',lit(current_timestamp()))
req_cols.show(5, False)


#Writing Dat into Delta Tables
df1_delta=req_cols.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("practicedb.productinventory1")

# COMMAND ----------

# Query-2
file_location2 = "/mnt/adventureworks2022/Person/PersonPerson"
df2 = spark.read.format("parquet").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load(file_location2)

# Query
from pyspark.sql.functions import *
req_cols = df2.select('BusinessEntityID', 'FirstName', 'LastName').filter((df2.PersonType == 'IN') & (df2.LastName == 'Adams')).orderBy('FirstName').withColumn('PersistLastModifiedTime',lit(current_timestamp()))
req_cols.show(5, False)

#Writing Dat into Delta Tables
#df2_delta=req_cols.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("practicedb.query2")


# COMMAND ----------

#Query-3
df3 = spark.read.format("parquet").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load("/mnt/adventureworks2022/Person/PersonPersonPhone")
#df2.show(5,False)
#df3.show(5,False)

#Query
req_cols3=df2.join(df3,'BusinessEntityID').select(df2.BusinessEntityID,df2.FirstName,df2.LastName,df3.PhoneNumber).orderBy(df2.LastName,df2.FirstName).filter(col('LastName').like("L%"))
req_cols3.show(5,False)
