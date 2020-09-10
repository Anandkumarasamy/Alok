from pyspark import SparkContext

sc=SparkContext('local[2]','Example of file content access')

text_file=sc.textFile('/home/anand/Desktop/PYSPARK_Program/Sample_file.txt')

word=text_file.flatMap(lambda x : x.split(' ')).map(lambda y : (y,1))

word_count=word.reduceByKey(lambda a,b : a+b)

print('Word count are:', word_count.count())

print('Word are:',word_count.collect())

#word_count.saveAsTextFile('/home/anand/Desktop/PYSPARK_Program/Sample_file_output.txt')