from pyspark import SparkContext

def loadBoringWords():
    name_set = set(words.strip() for words in open("D:/project-youtube/sparkPractice/boringwords.txt"))
    return name_set

sc = SparkContext("local[*]","Remove Boring Word")

boring_name_set = sc.broadcast(loadBoringWords())

input_rdd = sc.textFile("D:/project-youtube/sparkPractice/bigdata-campaign-data.csv")

mapped_rdd = input_rdd.map(lambda x:(float(x.split(",")[10]),x.split(",")[0]))

words_rdd = mapped_rdd.flatMapValues(lambda x:x.split(" "))

lower_words_rdd = words_rdd.map(lambda x:(x[1].lower(),x[0]))

final_words = lower_words_rdd.filter(lambda x: x[0] not in boring_name_set.value)

words_reduce_rdd = final_words.reduceByKey(lambda x,y:x+y)

sorted_words = words_reduce_rdd.sortBy(lambda x:x[1],False)

result_rdd = sorted_words.take(20)

for result in result_rdd:
    print(result)