from pyspark import SparkContext

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2)

pairs = data.map(lambda line: line.split("\t"))
grouped = pairs.groupByKey().map(lambda x: (x[0], list(x[1])))

output = grouped.collect()
for user, pages in output:
    print ("User %s viewed pages %s" % (user, pages))
print ("User page views done")

sc.stop()