from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col, column, udf

spark_conf = SparkConf()
spark_conf.set("spark.app.name","column add prg")
spark_conf.set("spark.master","local[2]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# data_schema = StructType([
#     StructField("name",StringType()),
#     StructField("age",IntegerType()),
#     StructField("city",StringType())
# ])

def ageCheck(age):
    if age>18:
        return "Y"
    else:
        return "N"

data_df = spark.read \
    .format("csv") \
    .option("inferSchema",True) \
    .option("path","D:\project-youtube\sparkPractice\Week12\dataset1") \
    .load()

parseAgeFunction = udf(ageCheck)

data_df = data_df.toDF("name","age","city")

# data_df.printSchema()
new_data_df = data_df.withColumn("is_adult",parseAgeFunction(column("age")))

new_data_df.show()