from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.master","local[2]")
spark_conf.set("spark.app.name","SparkSQLEx1")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()

orders_df = spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path","D:/project-youtube/sparkPractice/Week12/orders.csv") \
    .load()

orders_df.createOrReplaceTempView("orders")

# result_df = spark.sql("select * from orders")
# result_df = spark.sql("select order_status,count(*) as count_status from orders group by order_status")

result_df = spark.sql("select order_customer_id,count(*) as order_count from orders where "
                      "order_status='CLOSED' group by order_customer_id order by order_count")

orders_df.write \
    .format("csv") \
    .mode("overwrite") \
    .bucketBy(4,"order_customer_id") \
    .sortBy("order_customer_id") \
    .saveAsTable("orders_table")

result_df.show()