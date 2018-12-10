from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# http://www.cse.ust.hk/msbd5003/data/Customer.csv
# http://www.cse.ust.hk/msbd5003/data/SalesOrderDetail.csv
# http://www.cse.ust.hk/msbd5003/data/SalesOrderHeader.csv

dfCustomer = spark.read.csv('Customer.csv', header=True, inferSchema=True)
dfDetail = spark.read.csv('SalesOrderDetail.csv', header=True, inferSchema=True)
dfHeader = spark.read.csv('SalesOrderHeader.csv', header=True, inferSchema=True)

#Calculate the Table with customer ID and their spending.

TotalPrice = dfDetail.select('*',(dfDetail.UnitPrice*dfDetail.OrderQty*(1-dfDetail.UnitPriceDiscount)).alias('netprice'))   \
    .groupBy('SalesOrderID').sum('netprice').withColumnRenamed('sum(netprice)','TotalPrice')

CustomID = dfCustomer.join(dfHeader,'CustomerID','left').select('CustomerID','SalesOrderID')

df = CustomID.join(TotalPrice,'SalesOrderID','left').groupBy('CustomerID').sum('TotalPrice')    \
    .withColumnRenamed('sum(TotalPrice)','TotalPrice').na.fill(0)

#Calculate the average spending for all customer.

mean = df.groupBy().avg("TotalPrice").take(1)[0][0]

#Filter for the final answer.

df = df.filter(df.TotalPrice > mean).sort("TotalPrice",ascending=False)