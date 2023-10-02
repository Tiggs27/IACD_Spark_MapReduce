from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("TotalSpentOrder")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    costumer_id = str(fields[0])
    amount = float(fields[2])
    return (costumer_id, amount)

lines = sc.textFile("customer-orders.csv")

rdd = lines.map(parseLine)

totalsbycostumer = rdd.reduceByKey(lambda x,y :x+y)


results = totalsbycostumer.sortBy(lambda x:  [x[1], x[0]]).collect()

for result in results:
    print(result)