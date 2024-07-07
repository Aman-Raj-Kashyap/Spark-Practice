# finding the frequency of each word

from pyspark import SparkContext
sc = SparkContext("local","Word Count")

sc.setLogLevel("ERROR") #WARN | ERROR | INFO | FATAL

input = sc.textFile("D:/project-youtube/sparkPractice/search_data.txt")

words = input.flatMap(lambda x : x.split(" "))

words_lower = words.map(lambda x : x.lower())

words_map = words_lower.map(lambda x : (x,1))

words_count = words_map.reduceByKey(lambda x,y : x+y)

# words_reversed = words_count.map(lambda x : (x[1],x[0]))

# sorted_words = words_reversed.sortByKey(False)

# original_sorted_words = sorted_words.map(lambda x : (x[1],x[0]))

sorted_words = words_count.sortBy(lambda x : x[1],False)

print(sorted_words.collect())