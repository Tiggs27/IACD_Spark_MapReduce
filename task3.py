from pyspark import SparkConf, SparkContext
import collections


conf = SparkConf().setMaster("local").setAppName("Book_word")
sc = SparkContext(conf=conf)


lines = sc.textFile("Book")

count_words = lines.flatMap(lambda x: x.split(" "))\
                .map(lambda x : (x,1))\
                .reduceByKey(lambda x,y:x+y)

results = count_words.sortBy(lambda x:  [x[1], x[0]]).collect()


for result in results:
    print(result)   