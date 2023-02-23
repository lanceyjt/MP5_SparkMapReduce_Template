#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)


#TODO
lines_list = lines.collect()
counts = []

for line in lines_list:
    # TODO
    line = line.strip()
    _, count = line.split('\t')
    counts.append(int(count))

sum_val = int(sum(counts))
mean_val = int(sum_val/len(counts))
min_val = int(min(counts))
max_val = int(max(counts))

temp_lst = [pow(x-mean_val, 2) for x in counts]
var_val = int(sum(temp_lst)/len(counts))


outputFile = open(sys.argv[2], "w")
'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''
outputFile.write('Mean\t%s\n' % mean_val)
outputFile.write('Sum\t%s\n' % sum_val)
outputFile.write('Min\t%s\n' % min_val)
outputFile.write('Max\t%s\n' % max_val)
outputFile.write('Var\t%s\n' % var_val)

outputFile.close()
sc.stop()

