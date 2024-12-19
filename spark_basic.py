import sys
print(sys.version)
import os
os.environ["SPARK_HOME"] = r"C:\Users\2024035\AppData\Local\Programs\Python\Python37\Lib\spark-2.4.8-bin-hadoop2.7"
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="SparkExample")
list = [1,2,3,4]
rdd = sc.parallelize(list)
rdd.collect()
rdd = sc.parallelize(range(0,100))
rdd.saveAsTextFile('range_to_100')
out = rdd.collect()

print(out)
#map and flatMap

wordList = [['cat','cat'],['ele'],['cat','ele']]
rdd = sc.parallelize(wordList)
output = rdd.map(lambda line:[x for x in line].collect()) #list를 개별적으로 출력
output = rdd.flatMap(lambda line:[x for x in line].collect()) #list를 하나로 출력
print(output)
#filter

def remove_cat(animal):
    return(animal != 'cat')

wordList = [['cat','cat'],['ele'],['cat','ele']]
rdd = sc.parallelize(wordList)
rdd.flatMap(lambda line: [x for x in line]) \
.filter(remove_cat) \
.collect()
#aggregation

def remove_cat(animal):
    return(animal != 'cat')

wordList = [['cat','cat'],['ele'],['cat','ele']]
rdd = sc.parallelize(wordList)
rdd.flatMap(lambda line: [x for x in line]) \
.filter(remove_cat) \
.map(lambda ele: (ele,1)) \
.groupByKey() \
.mapValues(lambda x : sum(x)) \
.collect()
rdd = sc.parallelize([("a",1),("b",1),("a",1)])
sorted(rdd.groupByKey().mapValues(len).collect())
rdd = sc.parallelize([("a",1),("b",1),("a",1)])
sorted(rdd.groupByKey().mapValues(list).collect())
#aggregation

def remove_cat(animal):
    return(animal != 'cat')

wordList = [['cat','cat'],['ele'],['cat','ele']]
rdd = sc.parallelize(wordList)
rdd.flatMap(lambda line: [x for x in line]) \
.filter(remove_cat) \
.map(lambda ele: (ele,1)) \
.groupByKey() \
.mapValues(lambda x : x,y: x+y) \
.collect()
#join(leftouterjoin, rightouterjoin)

rdd1 = sc.parallelize([("a",1),("b",4)])
rdd2 = sc.parallelize([("a",2)])
sorted(rdd2.leftOuterJoin(rdd1).collect())
# actions

wordList = [['cat','cat'],['ele'],['cat','ele']]
rdd = sc.parallelize(wordList)
output = rdd.flatMap(lambda line: [x for x in line].countByValue())
output = rdd.flatMap(lambda line: [x for x in line].take())

print(output)
