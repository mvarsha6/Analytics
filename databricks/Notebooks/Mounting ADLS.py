# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://databrics@adlsdwdev.blob.core.windows.net/",
    mount_point='/mnt/databrics',
    extra_configs={
        "fs.azure.account.key.adlsdwdev.blob.core.windows.net": 
            'sbdV10I9xQ/icSc17/AI0WCRBpO4AHLXHBJjmTJ3B4PEzXNUkzFvOispc36+cmT9KqOthBeErl9C+ASt9gUrAw=='            
    }
)

# COMMAND ----------

dbutils.fs.ls('/mnt/databrics')

# COMMAND ----------


