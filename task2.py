from pyspark import SparkConf, SparkContext
import collections


conf = SparkConf().setMaster("local").setAppName("Book_word")
sc = SparkContext(conf=conf)


lines = sc.textFile("Book")

count_words = lines.flatMap(lambda x: x.split(" "))\
                .map(lambda x : (x,1))\
                .reduceByKey(lambda x,y:x+y)


for result in count_words.collect():
    print(result)   