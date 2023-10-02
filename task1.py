from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MinimumTemperaturePerCapital")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    Station = str(fields[0])
    ob_type = str(fields[2])
    temp = float(fields[3]) / 10
    return (Station, ob_type,temp)

lines = sc.textFile("1800.csv")

rdd = lines.map(parseLine)

filter_tmin = rdd.filter(lambda x: x[1] == 'TMIN')

reduction = filter_tmin.map(lambda x: (x[0],x[2]))

minbystation = reduction.reduceByKey(lambda x,y: min(x,y))

results = minbystation.collect()

for result in results:
    print(result)