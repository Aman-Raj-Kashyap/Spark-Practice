# user_id | movie_id | rating_given | timestamp

# find no. of each star (how many time 5,4,3,2,1 star rated)

from pyspark import SparkContext

sc = SparkContext("local[*]","RatingCalculator")

rdd1 = sc.textFile("D:\project-youtube\sparkPractice\moviedata.data")

rdd2 = rdd1.map(lambda x:x.split("\t")[2])

# rdd3 = rdd2.map(lambda x:(x,1))
#
# rdd4 = rdd3.reduceByKey(lambda x,y:x+y)
#
# rdd5 = rdd4.sortBy(lambda x:x[1])
#
# print(rdd5.collect())


print(rdd2.countByValue())