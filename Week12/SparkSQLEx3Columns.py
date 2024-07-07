from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import column,col,expr
from pyspark.sql.types import StructType,StructField,LongType,StringType,TimestampType

spark_conf = SparkConf()
spark_conf.setAppName("Column Object")
spark_conf.setMaster("local[2]")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

orders_schema = StructType([
    StructField("order_id",LongType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",LongType()),
    StructField("order_status",StringType())
])

orders_df = spark.read \
    .format("csv") \
    .option("header",True) \
    .schema(orders_schema) \
    .option("path","D:/project-youtube/sparkPractice/Week12/orders.csv") \
    .load()

# orders_df.select("order_id","order_customer_id")
# orders_df.select(column("order_id"),col("order_customer_id")).show()

orders_df.select("order_id",expr("concat(order_status,'_STATUS') as new_status")).show(truncate=False)

# orders_df.selectExpr("order_id","concat(order_status,'_STATUS')").show()