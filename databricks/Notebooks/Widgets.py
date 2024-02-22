# Databricks notebook source
#Text
dbutils.widgets.text("Text","Hello World!")

#Dropdown menu
dbutils.widgets.dropdown("Dropdown","1",[str(i) for i in range(1,11)])

#Combobox
dbutils.widgets.combobox("Combobox","A",["A","B","C","D"])

#Multi-Select
dbutils.widgets.multiselect("MultiSelect","Yes",["Yes","No","Maybe"])

# COMMAND ----------

#Receiving the input to use in your notebook

text=dbutils.widgets.get("Text")
print(text)

dropdown=dbutils.widgets.get("Dropdown")
print(dropdown)

combobox=dbutils.widgets.get("Combobox")
print(combobox)

multiselect=dbutils.widgets.get("MultiSelect").split(",")
for i in multiselect:
    print(i)

# COMMAND ----------

#To remove widgets
dbutils.widgets.remove("Dropdown")
dbutils.widgets.removeAll()
