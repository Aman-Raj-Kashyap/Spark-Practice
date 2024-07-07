from pyspark import SparkContext

sc = SparkContext("local[*]","Concept")

print(sc.defaultParallelism)


my_list = ("WARN: Tuesday 4 September 0405",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408")

rdd=sc.parallelize(my_list)
print(rdd.getNumPartitions())

base_rdd = sc.textFile("D:/project-youtube/sparkPractice/week10/bigLog.txt")
print(base_rdd.getNumPartitions())

changed_rdd = base_rdd.repartition(20)
print(changed_rdd.getNumPartitions())

latest_rdd = changed_rdd.coalesce(15)
print(latest_rdd.getNumPartitions())

#repartition is to increase/decrease no.of partitions, however it is used for increasing partitions as it requires full shuffling
#coalesce is to decrease partitions as it tries to avoid shuffling, less shuffling involved