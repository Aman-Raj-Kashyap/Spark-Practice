from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local","Log Level Grouping")
sc.setLogLevel("ERROR")

base_rdd = sc.textFile("D:\project-youtube\sparkPractice\week10\LogLevelGrouping.py")

mapped_input = base_rdd.map(lambda x : (x.split(",")[0],float(x.split(",")[2])))

total_by_customer = mapped_input.reduceByKey(lambda x,y:x+y)

premium_customer = total_by_customer.filter(lambda x:x[1]>5000)

doubled_amount = premium_customer.map(lambda x:(x[0],x[1]*2))

result = doubled_amount.collect()

for a in result:
    print(a)

stdin.read()