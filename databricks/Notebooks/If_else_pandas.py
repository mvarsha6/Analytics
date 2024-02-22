# Databricks notebook source
import pandas as pd

# COMMAND ----------

weather_data =[
... 	{'day':'1/1/2017', 'temperature': 32, 'windspeed': 6, 'event': 'Rain'},
... 	{'day':'1/2/2017', 'temperature': 35, 'windspeed': 7, 'event': 'Sunny'},
... 	{'day':'1/3/2017', 'temperature': 28, 'windspeed': 2, 'event': 'Snow'},
        {'day':'1/4/2017', 'temperature': 27, 'windspeed': 6, 'event': 'Rain'},
... 	{'day':'1/5/2017', 'temperature': 25, 'windspeed': 7, 'event': 'Sunny'},
... 	{'day':'1/6/2017', 'temperature': 28, 'windspeed': 2, 'event': 'Snow'},
... ]
df = pd.DataFrame(weather_data)
df_rain=pd.DataFrame
df.head()


# COMMAND ----------

if df['event']=='Rain':
    df_rain.append(df[df.event == 'Rain'])



# COMMAND ----------

df_rain = df[df['event'] == 'Rain']
df_rain
