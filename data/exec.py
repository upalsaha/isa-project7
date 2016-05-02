from pyspark import SparkContext
from itertools import combinations

def combin(x, y):
	list = []
	combos = combinations(y, 2)
	for combo in combos:
		if combo[0] != combo[1]:
			list.append(((combo[0], combo[1]), x))
	return list


sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2)

#Read data in as pairs of (user_id, item_id clicked on by the user)
pairs = data.map(lambda line: line.split("\t"))
#Group data into (user_id, list of item ids they clicked on)
grouped = pairs.groupByKey() 
#Transform into (user_id, (item1, item2) where item1 and item2 are pairs of items the user clicked on
combos = grouped.flatMap(lambda x: combin(x[0], x[1]))
#Remove non-distinct users
nodup = combos.distinct()
#Transform into ((item1, item2), list of user1, user2 etc) where users are all the ones who co-clicked (item1, item2)
freq = nodup.groupByKey()
#Transform into ((item1, item2), count of distinct users who co-clicked (item1, item2)
count = freq.map(lambda x: (x[0], len(x[1])))
#Filter out any results where less than 3 users co-clicked the same pair of items
filtered = count.filter(lambda x: x[1] > 2)

output = filtered.collect()

for pages, count in output:
    print ("Combination: %s was viewed %s times" % (pages, count))
print ("User page views done")

sc.stop()

