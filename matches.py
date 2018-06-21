from __future__ import print_function

import sys
from operator import add
import requests
from pyspark.sql import SparkSession
from pyspark.sql import Row
from collections import OrderedDict
import simplejson as json 
from pyspark.sql.types import *

headers ={'Content-Type':'application/json'}



if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    response = requests.get("http://localhost:5000/matches/",headers=headers)
    data =response.json() 
    data = data.get('data',[])
    schema = StructType([
    StructField('city', StringType(), True),
    StructField('player_of_match', StringType(), True),
    StructField('season', StringType(), True),
    StructField('winner', StringType(), True),
    StructField('venue', StringType(), True),
    StructField('team1', StringType(), True),
    StructField('team2', StringType(), True),
    StructField('umpire3', StringType(), True),
    StructField('umpire2', StringType(), True),
    StructField('umpire1', StringType(), True),
    StructField('result', StringType(), True),
    StructField('win_by_runs', StringType(), True),
    StructField('date', StringType(), True),
    StructField('win_by_wickets', StringType(), True),
    StructField('dl_applied', StringType(), True),
    StructField('id', StringType(), True),
    StructField('toss_winner', StringType(), True),
    StructField('toss_decision', StringType(), True),
])
    #data = data[0:10]
    #rdd = spark.sparkContext.parallelize(data).toDF()
    #rdd.show()
    otherPeopleRDD = spark.sparkContext.parallelize(data).map((lambda x: Row(**OrderedDict(sorted(x.items())))))
    df = spark.createDataFrame(otherPeopleRDD, schema) 
    df.select('winner','player_of_match').show()
    df.where(df.winner == 'Mumbai Indians').show()
    #dtaDF.show()
    #rdd = rdd.map(lambda x: pyspark.sql.Row(**x))


    #./spark-submit --master spark://sarath.hp:7077 /home/sarath/Workspace/PyVersions/flaskenv/Project/flaskAPI/resources/mathces.py
