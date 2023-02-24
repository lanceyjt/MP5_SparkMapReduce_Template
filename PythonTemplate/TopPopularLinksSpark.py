#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100) 

#TODO
lines_lst = lines.collect()

page_freq = {}
for line in lines_lst:
  #TODO
  line = line.strip()
  _, dest = line.split(":")
  dests = [x.strip() for x in dest.split(' ') if x.strip()!='']
  for d in dests:
    page_freq[d] = page_freq.get(d,0)+1

page_freq_lst = list(page_freq.items())
page_freq_lst.sort(reverse=False, key=lambda x: (x[1], x[0]))

output_lines = ["{page}\t{count}".format(page=x[0], count=x[1]) for x in page_freq_lst[-10:]]
result = "\n".join(output_lines)

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
output.write(result)
output.close()

sc.stop()

