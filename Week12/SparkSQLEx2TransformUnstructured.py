from pyspark.sql import SparkSession
from pyspark import SparkConf
import re

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark_conf = SparkConf()
spark_conf.setAppName("Transform Unstructured Data with Regex")
spark_conf.setMaster("local[2]")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerid", StringType(), True),
    StructField("status", StringType(), True)
])

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

rdd = spark.sparkContext \
    .textFile("D:/project-youtube/sparkPractice/Week12/orders_new.csv")

def transform_to_csv(line):
    pattern = r'(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d+),(\w+)'
    match = re.match(pattern, line)
    if match:
        transformed = (int(match.group(1)), match.group(3), match.group(4))
        return transformed

csv_rdd = rdd.map(transform_to_csv)

result_df = spark.createDataFrame(csv_rdd,schema)

result_df.show()