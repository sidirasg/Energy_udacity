""""Energy cosomution"""

import pandas as pd

import cassandra
import csv
import os


def create_table_cass(TableName,propert1,type1,propert2, type2,propert3,type3,propert4,type4):
    session_table="CREATE TABLE IF NOT EXISTS "+TableName
    session_table=session_table+"("+propert1+" ,"+ type1+" ,"+propert2+" ,"+type2+" ,"+propert3+" ,"+type3+" ,"+"Primary Key("+propert1+","+propert3+"))"
    return session_table


## Create a Keyspace "kayspace is similar to cluster"
#make connection to a cassandra instance your local machine 127.0.0.1

from cassandra.cluster import  Cluster
cluster=Cluster()
session=cluster.connect()


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXIST evident
    WITH REPLICATION=
    {'class': 'SimpleStrategy', 'replication_factor' : 1} """)
except Exception as e:
    print(e)


#pint current directory
print(os.getcwd())
filepath='data/'

file="CC_LCL-FullData.csv"


#create the table
energy_consum=create_table_cass("energy_cossum","household_id","varchar","house_ctgr","varchar","timestamp","varchar","energy","float")

try:
    session.execute(energy_consum)
except Exception as e:
    print(e)



with open(filepath+file) as f:
    for i in f :
        csvreader=csv.reader(f)
        next(csvreader)
        for line in csvreader:
            q="""INSERT INTO energy_cossum (household_id,house_ctgr,timestamp,energy) """
            q=q+"""VALUES (%s,%s,%s,%s);"""
           # print()
            session.execute(q,str(line[0]),str(line[1]),str(line[2]),line[3])

session.shutdown()
cluster.shutdown()


