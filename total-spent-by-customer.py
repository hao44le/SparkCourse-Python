from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(",")
    return (int(fields[0]),float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("CustomerTotalSpent")
sc = SparkContext(conf=conf)

input = sc.textFile("customer-orders.csv")
parsedLine = input.map(parseLine)
totalCounts = parsedLine.reduceByKey(lambda x,y: x+y)
totalCountsSorted = totalCounts.map(lambda (x,y): (y,x)).sortByKey()

results = totalCountsSorted.collect()

for result in results:
    print result