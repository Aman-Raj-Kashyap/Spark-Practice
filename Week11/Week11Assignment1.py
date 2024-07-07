from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark_conf = SparkConf()
spark_conf.set("spark.app.name","Week 11 Asg Sol1")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

window_data_schema = StructType([
    StructField("country",StringType(),True),
    StructField("weeknum",IntegerType(),True),
    StructField("countryinvoices",IntegerType(),True),
    StructField("totalquantity",IntegerType(),True),
    StructField("invoicevalue",FloatType(),True)
])

window_data_df = spark.read \
    .format("csv") \
    .schema(window_data_schema) \
    .option("path","D:/project-youtube/sparkPractice/Week11/window_data_assignment.csv") \
    .load()

window_data_df.printSchema()

window_data_df.write \
    .format("parquet") \
    .partitionBy("country") \
    .option("path","D:/project-youtube/sparkPractice/Week11/AssignmentSol") \
    .save()