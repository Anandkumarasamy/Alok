from pyspark import SparkContext

sc=SparkContext('local[2]','Map amd FlateMap example')

lis1=list(range(1,11))

lis1_rdd=sc.parallelize(lis1)

squ_rdd=lis1_rdd.map(lambda x : [x, x*x])

cub_rdd=lis1_rdd.flatMap(lambda x : [x, x*x*x])

print('Square number:',squ_rdd.collect())

print('Square number:',squ_rdd.count())

print('Cube number:',cub_rdd.collect())

print('Cube number:',cub_rdd.count())

'''file_Content=sc.textFile('Sample.txt')

line_map=file_Content.map(lambda X : X.split(' '))

line_flatMap=file_Content.flatMap(lambda X : X.split(' '))

print('word counts for map method:',line_map.count())

print('String for map method:',line_map.collect())

print('word counts for flatMap method:',line_flatMap.count())

print('String for flatMap method:',line_flatMap.collect())'''



