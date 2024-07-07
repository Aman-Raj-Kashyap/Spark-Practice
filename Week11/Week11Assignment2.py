from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark_conf = SparkConf()
spark_conf.set("spark.app.name","Week 11 Asg Sol 2")
spark_conf.set("spark.master","local[2]")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

window_data_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Weeknum", IntegerType(), True),
    StructField("NumInvoices", IntegerType(), True),
    StructField("TotalQuantity", IntegerType(), True),
    StructField("InvoiceValue", StringType(), True)
])

window_data_df = spark.sparkContext \
    .textFile("D:/project-youtube/sparkPractice/Week11/window_data_assignment.csv") \
    .map(lambda x:x.split(",")) \
    .map(lambda x: (x[0],int(x[1].strip()),int(x[2].strip()),int(x[3].strip()),x[4])) \
    .toDF(window_data_schema) \
    .repartition(8)

window_data_df.write \
    .format("json") \
    .mode("overwrite") \
    .partitionBy("Country") \
    .option("maxRecordsPerFile",2000) \
    .option("path","D:/project-youtube/sparkPractice/Week11/Assignment2Sol") \
    .save()

spark.stop()

