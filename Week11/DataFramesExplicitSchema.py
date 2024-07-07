from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark.sql.types import StructType,StructField,TimestampType,LongType,StringType

ordersSchema = StructType([StructField("orderid",LongType()),
                           StructField("orderdate",TimestampType()),
                           StructField("customerid",LongType()),
                           StructField("status",StringType())
                        ])


ordersDDL = """orderidnew Integer, orderdate timestamp, customerid Integer, status String"""

sparkConf = SparkConf()
sparkConf.set("spark.app.name","ExplicitSchema")
sparkConf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

ordersDf = spark.read\
    .format("csv")\
    .schema(ordersDDL)\
    .option("path","D:/project-youtube/sparkPractice/Week11/orders.csv")\
    .load()

ordersDf.printSchema()
ordersDf.show()