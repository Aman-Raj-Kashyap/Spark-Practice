from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local","Log Level Count ReduceBy")

sc.setLogLevel("ERROR")

base_rdd = sc.textFile("D:/project-youtube/sparkPractice/week10/bigLog.txt")

mapped_rdd = base_rdd.map(lambda x:(x.split(":")[0],1))

grouped_rdd = mapped_rdd.reduceByKey(lambda x,y:x+y)

result = grouped_rdd.collect()

for a in result:
    print(a)
