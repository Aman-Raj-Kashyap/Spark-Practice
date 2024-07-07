from pyspark.sql import SparkSession
from pyspark import SparkConf

sparkConf = SparkConf()
sparkConf.set("spark.app.name","DataFrameVSDataSets")
sparkConf.set("spark.master","local[2]")


spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()

# orders_df = spark.read\
#     .format("csv")\
#     .option("header",True)\
#     .option("inferSchema",True)\
#     .option("path","D:/project-youtube/sparkPractice/Week11/orders.csv")\
#     .load()

orders_df = spark.read\
    .format("json")\
    .option("path","D:/project-youtube/sparkPractice/Week11/players.json")\
    .option("mode","DROPMALFORMED")\
    .load()
#PERMISSIVE,DROPMALFORMED,FAILFAST


orders_df_parquet = spark.read\
    .option("path","D:/project-youtube/sparkPractice/Week11/users.parquet")\
    .load()

orders_df_parquet.printSchema()
orders_df_parquet.show()