import cassandra

from  cassandra.cluster import Cluster
try:
    cluster=Cluster(['127.0.0.1'])
    session=cluster.connect()
except Exception as e:
    print(e)

#we colould Create a space with try if there is not the keyspace to ccreate but we do not do  for now

#connect to keyspace
try:
    session.set_keyspace('evident')
except Exception as e:
    print(e)

query="select * from energy_cossum;"


try:
    rows=session.execute(query)
except Exception as e:
    print(e)





import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
df = spark.read.option("header",True).csv("data/CC_LCL-FullData.csv")
df.printSchema()

df = df.withColumn("DateTime",df.DateTime.astype('Timestamp'))
#https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/
df3=df.select(col("DateTime"),
             col("LCLid").alias(("id")),
            col("stdorToU").alias("std"),
            col("KWH/hh (per half hour) ").alias("kwh"),
     year(col("DateTime")).alias("year"),
     month(col("DateTime")).alias("month"),
    dayofmonth(col("DateTime")).alias("date"),
    hour(col("DateTime")).alias("hour"),
    minute(col("DateTime")).alias("min"),
  )







import pandas as pd

from datetime import datetime
import calendar
import warnings
warnings.filterwarnings("ignore")


# Reading Weather data
df_weather = pd.read_csv("data/add/weather_hourly_darksky.csv")

# Creating date, time related columns
df_weather = df_weather[['temperature', 'time']]
df_weather.columns = ['temperature', 'DateTime']
df_weather['DateTime'] = pd.to_datetime(df_weather['DateTime'])
df_weather['year'] = df_weather['DateTime'].dt.year
df_weather['month'] = df_weather['DateTime'].dt.month
df_weather['day'] = df_weather['DateTime'].dt.day
df_weather['time'] = df_weather['DateTime'].dt.time



#### Weather information for the year 2013
df_weather_2013 = df_weather[df_weather.year==2013][['temperature', 'month']]
df_weather_2013 = df_weather_2013.groupby(by=['month'])['temperature'].mean().to_frame()
df_weather_2013.reset_index(level=0, inplace=True)
df_weather_2013.month = df_weather_2013.month.apply(lambda x: calendar.month_abbr[x])


#### Weather information for the year 2014
df_weather_2014 = df_weather[df_weather.year==2014][['temperature', 'month']]
df_weather_2014 = df_weather_2014.groupby(by=['month'])['temperature'].mean().to_frame()
df_weather_2014.reset_index(level=0, inplace=True)
df_weather_2014.month = df_weather_2014.month.apply(lambda x: calendar.month_abbr[x])


# Reading Household info data
df_household = pd.read_csv("data/add/informations_households.csv", encoding="utf-8")



#visualization

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import calendar

#filter the data
df3.filter(df3.id=='MAC000002').count()

#household convert spark  dataframe
householdd=spark.createDataFrame(df_household)

#Join two DataSet
df4=df3.join(householdd, df3.id == householdd.LCLid, 'left')
df5=df4.withColumn("kwh", df4.kwh.cast('double'))


#convert string energy cosumtion data to double
df5=df4.withColumn("kwh", df4.kwh.cast('double'))

#filter the data to perform to  a smaller sample all the
dff=df3.filter(df3.id=='MAC000002')


#innerjoin with household and energy
#Apply filter to have better result
df6=dff.join(householdd, dff.id == householdd.LCLid, 'left')
#convert string to Chriss
df7=df6.withColumn("kwh", df6.kwh.cast('double'))

df8=df7.groupby("id","std","year","month","date","hour").sum("kwh")


print("Before casting")
df8.printSchema()
df8_hours=df8.withColumn("hours",df8['hour'].cast('integer'))
print("After casting")
df8_hours.printSchema()

pivot_df = df8_hours.groupby("id","year","month","date").pivot("hours").sum("sum(kwh)")


#apply for all data in the dataset the aggregation sums

#first we aggregate in hours , in this data set we have every half hour so we sum every hour
# In the Eveddent dataset we do not need to aggredate every hour
df_all8=df5.groupby("id","std","year","month","date","hour").sum("kwh")
#make ne aggregation per data


from pyspark.sql import functions as F

df_all_statistics=df5.groupby("id","std","year","month","date").agg(f.sum("kwh"),f.avg("kwh"),f.max("kwh"),f.min("kwh"),f.count("kwh"),f.stddev_pop("kwh"))


print("Before casting")
df_all8.printSchema()

df_all_hours=df_all8.withColumn("hours",df_all8['hour'].cast('integer'))
print("After casting")
df_all_hours.printSchema()
pivot_df_all=df_all_hours.groupby("id","year","month","date").pivot("hours").sum("sum(kwh)")
#dataset = pivot_df_all.groupby("id","year","month","date").sum("sum(kwh)").avg("sum(kwh)")("sum(kwh)").min("sum(kwh)").count("sum(kwh)").std("sum(kwh)")

pivot_df_all.count()


dataset=pivot_df_all.join(id, pivot_df_all.id == df_all_statistics.id, 'left')