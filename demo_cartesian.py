from pyspark import SparkContext

sc=SparkContext('local[2]','Cartesian example')

lis_rdd1=sc.parallelize(range(1,7))

lis_rdd2=sc.parallelize(range(7,13))

lis_rdd3=sc.parallelize(range(13,19))

cartes_rdd1=lis_rdd1.cartesian(lis_rdd2)

cartesian_with3rdd=cartes_rdd1.cartesian(lis_rdd3)

print(cartes_rdd1.count())

print(cartes_rdd1.collect())

print(cartesian_with3rdd.count())

print(cartesian_with3rdd.collect())