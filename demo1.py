from pyspark import SparkConf , SparkContext
'''import numpy as np

num_lis=np.random.randn(5,5)

print(num_lis)'''

conf = SparkConf().setAppName('Sample program').setMaster('local[2]')

sc=SparkContext(conf=conf)

data = [('A', 2),('A', 4),('A', 9),('B', 10),('B', 20),('Z', 3),('Z', 5),('Z', 8),('Z', 12)]

rdd = sc.parallelize( data )

sumCount = rdd.combineByKey(lambda value: (value, 1),
                            lambda x, value: (x[0] + value, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1])
                           )

averageByKey = sumCount.map(lambda (key, (totalSum, count)): (key, totalSum / count))

print('Avg:\n',averageByKey.collectAsMap())




