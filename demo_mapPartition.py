from pyspark import SparkContext

sc=SparkContext('local[2]','MapPartition example')

num_rdd=sc.parallelize(list(range(1,11)),2)

#nun_rdd_mapParti=num_rdd.mapPartitions()

print(num_rdd.collect())

print('Number of Partition:',num_rdd.getNumPartitions())

print('Number of Partition:',num_rdd.foreach()