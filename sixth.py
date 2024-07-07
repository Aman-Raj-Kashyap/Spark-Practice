# row_id | name | age | number_of_connections
# 0::Will::33::385
# 1::Jean-Luc::26::2
# 2::Hugh::55::221

from pyspark import SparkContext

sc = SparkContext("local[*]","AvgConnectionsEachAge")

input = sc.textFile("D:\project-youtube\sparkPractice\friendsdata.csv")

mapped_input = input.map(lambda x : (int(x.split("::")[2]),int(x.split("::")[3])))

#since changes are happening at value part
#mapped_final = mapped_input.map(lambda x : (x[0],(x[1],1)))
mapped_final = mapped_input.mapValues(lambda x : (x,1))

total_by_age = mapped_final.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

#avg_by_age = total_by_age.map(lambda x:(x[0][0],x[1][0]/x[1][1]))
avg_by_age = total_by_age.mapValues(lambda x : x[0]/x[1])

print(avg_by_age.collect())
