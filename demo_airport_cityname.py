import re
from pyspark import SparkConf , SparkContext
import sys
sys.path.insert(0, '.')

class Utils():

    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    airports = sc.textFile('git_example/python-spark-tutorial/in/airports.text')

    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    
    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    '''airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")'''
    print(airportsNameAndCityNames.collect())


