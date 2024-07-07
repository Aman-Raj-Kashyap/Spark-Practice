# find out top 10 customers who spent maximum amount

#customer_id, product_id, amount_spent

# from pyspark import SparkContext
#
# sc = SparkContext("local","Max Spent Customer")
# input = sc.textFile("D:/project-youtube/sparkPractice/customer_orders.csv")
# mapped_input = input.map(lambda x : (x.split(",")[0],float(x.split(",")[2])))
# rdd3 = mapped_input.reduceByKey(lambda x,y : x+y)
# rdd4 = rdd3.sortBy(lambda x : x[1],False)
# print(rdd4.take(10))


from pyspark import SparkContext

sc=SparkContext("local","MaxSpentCustomer")
rdd1=sc.textFile("D:/project-youtube/sparkPractice/customer_orders.csv")
rdd2=rdd1.map(lambda x:(x.split(",")[0],float(x.split(",")[2])))
rdd3=rdd2.reduceByKey(lambda x,y:x+y)
rdd4=rdd3.sortBy(lambda x:x[1],False)
result=rdd4.collect()
for a in result:
    print(a)