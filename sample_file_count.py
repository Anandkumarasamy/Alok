fil=open("git_example/python-spark-tutorial/in/sample.txt","r")
#dic={}
content=fil.readlines()
str_content=str(content)
lis_country=[]
for country in list(str_content.split(',')):
    '''if word in dic.keys():
        dic[word]=(dic[word])+1
    else:
        dic[word]=1'''
    lis_country.append(country)
    print("Country are :",lis_country[3])