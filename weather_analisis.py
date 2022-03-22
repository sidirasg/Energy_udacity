import pandas as pd
import numpy as no
import  matplotlib.pyplot as plt
import  seaborn as sns

## Create a Keyspace "kayspace is similar to cluster"
#make connection to a cassandra instance your local machine 127.0.0.1

from cassandra.cluster import  Cluster
cluster=Cluster()
session=cluster.connect('evident')


 #weather_hourly="select si* from evident.weather_hourly_darksky;"

#weather_hourly=session.execute("select * from weather_hourly_darksky;")

weather_hourly  =pd.DataFrame(list(session.execute("select * from weather_hourly_darksky;")))