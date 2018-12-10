
# coding: utf-8
from pyspark import SparkContext
from pyspark.sql import SparkSession

# pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

sc = SparkContext()

# sc.addPyFile("../graphframes-0.5.0-spark2.1-s_2.11.jar")

from graphframes import *
from graphframes import GraphFrame
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .getOrCreate()

v = spark.createDataFrame([
 ("a", "Alice", 34),
 ("b", "Bob", 36),
 ("c", "Charlie", 37),
 ("d", "David", 29),
 ("e", "Esther", 32),
 ("f", "Fanny", 38),
 ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edges DataFrame
e = spark.createDataFrame([
 ("a", "b", "follow"),
 ("a", "c", "friend"),
 ("a", "g", "friend"),
 ("b", "c", "friend"),
 ("c", "a", "friend"),
 ("c", "b", "friend"),
 ("c", "d", "follow"),
 ("c", "g", "friend"),
 ("d", "a", "follow"),
 ("d", "g", "friend"),
 ("e", "a", "follow"),
 ("e", "d", "follow"),
 ("f", "b", "follow"),
 ("f", "c", "follow"),
 ("f", "d", "follow"),
 ("g", "a", "friend"),
 ("g", "c", "friend"),
 ("g", "d", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)

g.find('(a)-[e]->(c);(b)-[g]->(c);(b)-[f]->(a)').filter('g.relationship == "friend"').filter('e.relationship == "friend"') \
.filter('f.relationship == "follow"').select('a.name','b.name').distinct().show()