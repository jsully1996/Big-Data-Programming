from pyspark import SparkConf, SparkContext

import sys
import random
import operator

no_of_samples =int(sys.argv[1]) #define the input as a global variable

def get_count(iter):
    random.seed()  #saves the generated random numbers 
    count = 0    
    for i in range(int(iter)):
        sum = 0.0                  #representation of given pseudocode
        while (sum < 1):
            sum += random.random()
            count += 1
    return count

def get_iterations(partitions,iterations):
    list = []                           #returns a list with the all iterations for a single partition
    for i in range(partitions):         #Hence, the cost of calling functions is minimized
        list.append(iterations)  
    return list

def main():
    numparts = 100
    iteration_no = no_of_samples/numparts
    lst = get_iterations(numparts,iteration_no)
    iteration_rdd = sc.parallelize(lst, numparts).glom().map(get_count) #parallelize as per the hardcoded number of partitions
    s = iteration_rdd.reduce(operator.add)
    result = float(s)/no_of_samples
    print(result)

if __name__ == '__main__':
    conf = SparkConf().setAppName("euler")
    sc = SparkContext(conf=conf)
    main()
