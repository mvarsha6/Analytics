# Databricks notebook source
import pyodbc
pyodbc.drivers()



# COMMAND ----------

import pyodbc
server = 'VARSHAKOMMINENI\SQLEXPRESS'
database = 'AdventureWorks2022'
username = 'mvarsha.666'
password = 'Karthik@93'

# COMMAND ----------

cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE='+ database +';UID=' + username + ';PWD='+ password)

# COMMAND ----------


