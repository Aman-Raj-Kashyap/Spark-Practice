from pyspark import SparkContext

sc = SparkContext("local","CalculateAdCost")

rdd1 = sc.textFile("D:/project-youtube/sparkPractice/bigdata-campaign-data.csv")

rdd2 = rdd1.map(lambda x:(float(x.split(",")[10]),x.split(",")[0]))

rdd3 = rdd2.flatMapValues(lambda x:(x.split(" ")))

rdd4 = rdd3.map(lambda x:(x[1].lower(),x[0]))

rdd5 = rdd4.reduceByKey(lambda x,y:x+y)

rdd6 = rdd5.sortBy(lambda x:x[1],False)

result = rdd6.collect()

for x in result:
    print(x)


function loadBoringWords:

    lines = Source.fromFile("D:/project-youtube/sparkPractice/boringwords.txt").getLines()
