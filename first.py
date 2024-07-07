from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local", "Simple App")

    rdd1 = sc.textFile("D:/project-youtube/sparkPractice/file1.txt")

    rdd2 = rdd1.flatMap(lambda x: x.split(" "))

    rdd3 = rdd2.map(lambda x: (x, 1))

    rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

    result = rdd4.collect()

    for a in result:
        print(a)

    #print(rdd4.take(10))
    #rdd4.saveAsTextFile("output")

else:
    print("Not executed directly")


stdin.read()