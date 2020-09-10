from pyspark import SparkConf , SparkContext
'''import numpy as np

num_lis=np.random.randn(5,5)

print(num_lis)'''

conf = SparkConf().setAppName('Sample program').setMaster('local[2]')

sc=SparkContext(conf=conf)

users = sc.parallelize([(0, "Alex"), (1, "Bert"), (2, "Curt"), (3, "Don")])
hobbies = sc.parallelize([(0, "writing"), (0, "gym"), (1, "swimming")])

print('Result of the join are',users.join(hobbies).collect())
# [(0, ('Alex', 'writing')), (0, ('Alex', 'gym')), (1, ('Bert', 'swimming'))]

print('Result of the join+map are',users.join(hobbies).map(lambda x: x[1][0] + " likes " + x[1][1]).collect())
# ['Ale x likes writing', 'Alex likes gym', 'Bert likes swimming']

print('result of the left join are',users.leftOuterJoin(hobbies).collect())
# [(0, ('Alex', 'writing')), (0, ('Alex', 'gym')), (1, ('Bert', 'swimming')), (2, ('Curt', None)), (3, ('Don', None))]

print('Result 0f right join are',users.rightOuterJoin(hobbies).collect())

print('Result 0f cogroup are',users.cogroup(hobbies).collect())


