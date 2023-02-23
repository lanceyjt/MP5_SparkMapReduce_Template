#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 
lines_lst = lines.collect()

#TODO
sources_set = set()
dests_set = {}

for line in lines_lst:
  # TODO
  line = line.strip()
  source, dest = line.split(":")
  source = source.strip()
  dests = [x.strip() for x in dest.strip().split(' ') if x.strip()!='']
  sources_set.add(source)
  for dest in dests:
    if source != dest:
        dests_set.add(source)

orphan_pages = list(sources.difference(dests))
orphan_pages = [int(x) for x in orphan_pages]
orphan_pages.sort()

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
output.write("\n".join(orphan_pages))
output.close()
sc.stop()

