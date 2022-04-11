import matplotlib.pyplot as plt

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import *
import calendar

import pandas as pd


def merge(acc, x):
    count = acc.count + 1
    sum = acc.sum + x
    return struct(count.alias("count"), sum.alias("sum"))



spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
df = spark.read.option("header",True).csv("data/CC_LCL-FullData.csv",inferSchema=True,header=True)
df.printSchema()

df = df.withColumn("DateTime",df.DateTime.astype('Timestamp'))


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


# Reading Household info data
df_household = pd.read_csv("data/add/informations_households.csv", encoding="utf-8")

#household convert spark  dataframe
householdd=spark.createDataFrame(df_household)

df_4=df3.withColumn("kwh", df3.kwh.cast('double'))
#Join two DataSet
df4=df3.join(householdd, df3.id == householdd.LCLid, 'left')

df5=df4.withColumn("kwh", df4.kwh.cast('double'))

dff_2012=df5.filter(df5.year=='2012')
dff_2013=df5.filter(df5.year=='2013')
dff_2014=df5.filter(df5.year=='2014')





df8_hours=dff_2012.withColumn("hours",dff_2012['hour'].cast('integer'))


Energy_Total2012 = dff_2012.groupby("year").sum("kwh").toDF()

#aggragate function and hours cosumtion at coloumns
pivot_df_2012 = df_4.groupby("id","year","month","date").pivot("hour").sum("kwh")

pd.set_option('display.float_format', lambda x: '%.5f' % x)

Energy_Months2012 = dff_2012.groupby("month").sum("kwh").orderBy("month").toPandas()

Energy_Months2012.plot(kind='barh',x='month',y='sum(kwh)',colormap='winter_r')



plt.show()

