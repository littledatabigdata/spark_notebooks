from math import sqrt
from pyspark import SparkContext

sc = SparkContext()


numPartitions = 10
P = (5000, 5000)
K = 10

points = sc.textFile('points.txt',numPartitions)
pairs = points.map(lambda l: tuple(l.split()))
pairs = pairs.map(lambda pair: (int(pair[0]),int(pair[1])))
pairs.cache()

# YOUR CODE BEGINS HERE
def distance(a,b): # define the function for distance between points
    return sqrt((a[0]-b[0])*(a[0]-b[0])+(a[1]-b[1])*(a[1]-b[1]))

T = pairs.map(lambda pair: (pair, distance(pair,P))) # map (point->(point, distance))

def f(iterator):
    result = [] # set the result list as empty
    max = ((0,0),0) # set the farthest point in the result list as 0.
    for i in iterator: 
        if (len(result) < K): # if the list is not full (Maximum = K)
            result.append(i) # insert new point into the list
            if (i[1] > max[1]) : # update the farthest point if possible
                max = i
        else : # if the list is full
            if (i[1] < max[1]) : # If the new point is closed to P than the farthest point in the list
                # Replace the farthest point with the new point
                result.remove(max) 
                result.append(i)
                # Renew the farthest point
                max = i
                for j in result:
                    if (j[1] > max[1]):
                        max = j
    # Report every point in the list
    for i in result:
        yield i

result = T.mapPartitions(f)
result = result.sortBy(lambda a: a[1]).map(lambda a:a[0]).take(K) # Sorting the K*numPartition elements and get the first K.


print result