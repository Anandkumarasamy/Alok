from pyspark import SparkConf , SparkContext

conf=SparkConf().setAppName('Page_Rank1').setMaster('local[2]')
sc=SparkContext(conf=conf)

page_with_links=sc.parallelize([("A B"), ("A C"), ("A D"), ("B A"), ("B C"), ("C D"), ("E B")])
#['A B', 'A C', 'A D', 'B A', 'B C', 'C D', 'E B']

page_groupup_links=page_with_links.flatMap(lambda x : x.split(',')).map(lambda x :(x[0],x[2])).groupByKey().mapValues(list)
#[('A', ['B', 'C', 'D']), ('C', ['D']), ('E', ['B']), ('B', ['A', 'C'])]

page_links_value=page_groupup_links.map(lambda x :(x[0],x[1],1.0))
#[('A', ['B', 'C', 'D'], 1), ('C', ['D'], 1), ('E', ['B'], 1), ('B', ['A', 'C'], 1)]

page_links_seperateValue=page_links_value.map(lambda x : (x[0],x[1],("%.4f"%(x[2]/len(x[1])))))

#print("%.2f" % a)


