from pyspark import SparkContext

sc = SparkContext("local","Word Count")

my_list = ["WARN: Tuesday 4 September 0405",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"WARN: Tuesday 4 September 0405"]

original_log_rdd = sc.parallelize(my_list)

pair_rdd = original_log_rdd.map(lambda x:(x.split(":")[0],1))

resultant_rdd = pair_rdd.reduceByKey(lambda x,y : x+y)

final_result = resultant_rdd.collect()

for a in final_result:
    print(a)