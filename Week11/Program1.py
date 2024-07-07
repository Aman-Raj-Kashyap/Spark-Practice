from pyspark import SparkContext, StorageLevel
from sys import stdin

sc = SparkContext("local[*]","Program 1")

base_rdd = sc.textFile("D:/project-youtube/sparkPractice/Week11/customer_orders.csv")

mapped_input = base_rdd.map(lambda x:((x.split(",")[0]),float(x.split(",")[2])))

total_by_customer = mapped_input.reduceByKey(lambda x,y:x+y)

premium_customer = total_by_customer.filter(lambda x:x[1] > 5000)

doubled_amount = premium_customer.map(lambda x:(x[0],x[1]*2)).persist(StorageLevel.MEMORY_ONLY)

result = doubled_amount.collect()

for a in result:
    print(result)

print(doubled_amount.count())

stdin.read()