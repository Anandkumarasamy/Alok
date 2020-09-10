from pyspark import SparkConf , SparkContext
import re
class Utils():

    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

conf=SparkConf().setAppName('hadoop in realworld').setMaster('local[2]')
sc=SparkContext(conf=conf)

text_file=sc.textFile('git_example/python-spark-tutorial/in/sample.txt')
split_ele=text_file.flatMap(lambda x : x.split(','))

print(split_ele.collect())

'''required_ele=split_ele.map(lambda y : y[3])

print(required_ele.collect())'''
