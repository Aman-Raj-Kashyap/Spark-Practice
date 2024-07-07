from pyspark import SparkContext

sc = SparkContext("local","Wide Tans vs Action")

a = range(1,101)

base_rdd = sc.parallelize(a)

result = base_rdd.reduce(lambda x,y:x+y)

print(result)