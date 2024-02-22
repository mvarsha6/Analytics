# Databricks notebook source
print("Hello World")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# define schema for dataframe
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# create data for dataframe
data = [("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35)]

# create dataframe
df = spark.createDataFrame(data, schema)

# show dataframe
df.show()


# COMMAND ----------

df.show(10,False)

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df

# COMMAND ----------

for i in range(5,0,-1):
    for k in range(i,6):
        print(k,end=" ")
    print()
for i in range(2,6):
    for k in range(i,6):
        print(k,end=" ")
    print()
print()
