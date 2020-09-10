from pyspark import SparkConf , SparkContext
#import org.apache.spark.HashPartitioner

conf=SparkConf().setAppName('Cogroup_Example').setMaster('local[2]')
sc=SparkContext(conf=conf)

rrd1=sc.parallelize([('a',3),('a',5),('c',7),('b',8),('a',6),('b',9),('c',4)],3)
rrd2=sc.parallelize([('d',2),('d',8),('b',5),('d',7)],2)

cogroup_rdd=rrd1.cogroup(rrd2)
for key,value in cogroup_rdd.collect():
    for num in value:
        print(key,':',num)