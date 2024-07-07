from pyspark.sql import SparkSession
from pyspark import SparkConf

sparkConf = SparkConf()
sparkConf.set("spark.master","local[2]")
sparkConf.set("spark.app.name","my first app")

# spark = SparkSession.builder\
#     .master("local[2]")\
#     .appName("My First App")\
#     .getOrCreate()

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()

#print(spark.sparkContext.getConf().getAll())
# logic

orders_df = spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("D:/project-youtube/sparkPractice/Week11/orders.csv")

# orders_df.show()
# orders_df.printSchema()

# print(orders_df.rdd.getNumPartitions())

#high level code
grouped_orders_df = orders_df\
    .repartition(4)\
    .where("order_customer_id > 10000")\
    .select("order_id","order_customer_id")\
    .groupby("order_customer_id")\
    .count()

grouped_orders_df.show()

#low level code
# grouped_orders_df.foreach(lambda x:print(x))

#working with raw rdd was a low level code
#Spark compiler convert high level code(Dataframe code) to lowlevel rdd code
#then it will send low level code to executor

spark.stop