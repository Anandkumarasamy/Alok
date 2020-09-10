from pyspark import SparkContext

sc=SparkContext('local[2]','Example for filter_RDD_Spark')

Officer_salary=[25000,30000,35000,40000,50000,65000,90000,70000,85000,20000,90000,85000,25000]

labour_salary=[6000,6500,7000,7500,8000,8500,9000,9500,1000,10000,2000,3500,5000,7000,8000,6000,4500]

total_employee_salary=Officer_salary+labour_salary

total_employee_salary_rdd=sc.parallelize(total_employee_salary)

total_officer_salary_Covidbonus=total_employee_salary_rdd.filter(lambda X : X>21000).map(lambda X : X+(0.10*X))

total_labour_salary_Covidbonus=total_employee_salary_rdd.filter(lambda X : X<15000).map(lambda X : 1.20*X)

total_employee_salary=total_officer_salary_Covidbonus.union(total_labour_salary_Covidbonus)

print(total_employee_salary.collect())



