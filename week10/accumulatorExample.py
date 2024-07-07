from pyspark import SparkContext

sc=SparkContext("local","Accumulator Example")

myrdd = sc.textFile("D:/project-youtube/sparkPractice/week10/samplefile.txt")

myaccum = sc.accumulator(0)

# res = myrdd.collect()
#
# for a in res:
#     if a == "":
#         myaccum.add(1)
#
# print(myaccum.value)

#we can't perform foreach on local vaiable in python but we can use it on RDD

def blankLineChecker(line):
    if len(line) == 0:
        myaccum.add(1)

myrdd.foreach(blankLineChecker)

print(myaccum.value)