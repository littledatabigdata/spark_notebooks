from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc = SparkContext()

ssc = StreamingContext(sc, 10)
ssc.checkpoint('checkpoints')

numPartitions = 8
rdd = sc.textFile('../numbers.txt', numPartitions)
rdd = rdd.map(lambda u: int(u))
rddQueue = rdd.randomSplit([1]*100, 123)
numbers = ssc.queueStream(rddQueue)

# YOUR CODE BEGINS HERE
Stat = numbers.map(lambda u:(u,1)).reduceByWindow(lambda x ,y: (x[0]+y[0],x[1]+y[1]), lambda x,y: (x[0]-y[0],x[1]-y[1]), 30, 10)

def printResult(rdd):
    result = rdd.take(1)
    print result[0][0]/result[0][1]

Stat.foreachRDD(printResult)

# YOUR CODE ENDS HERE

ssc.start()
ssc.awaitTermination(200)
ssc.stop(False)
print("Finished")