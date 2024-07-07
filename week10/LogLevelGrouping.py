from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local","Log Level Count GroupBy")

sc.setLogLevel("ERROR")

base_rdd = sc.textFile("D:/project-youtube/sparkPractice/week10/bigLog.txt")

mapped_rdd = base_rdd.map(lambda x:(x.split(":")[0],x.split(":")[1]))

result = mapped_rdd.groupByKey().map(lambda x:(x[0],len(x[1]))).collect()

for a in result:
    print(a[0],a[1])

stdin.read()
