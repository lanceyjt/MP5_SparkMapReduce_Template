#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	#TODO
    stop_words = [line.strip() for line in f]

with open(delimitersPath) as f:
    #TODO
    delimiters = f.read()
    delimiters = [c for c in delimiters]

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO
default_delimiter = delimiters[0]

word_count = {}
line_list = lines.collect()
for line in line_list:
    line = line.strip()
    for d in delimiters[1:]:
        line = line.replace(d, default_delimiter)
    word_lst = line.lower().split(default_delimiter)
    word_lst = [w for w in word_lst if w not in stop_words and w.strip() != '']
    for w in word_lst:
        word_count[w] = word_count.get(w,0)+1

word_freq = list(word_count.items())
word_freq.sort(reverse=False, key=lambda x: (x[1], x[0]))

output_lines = ["\t".join(x) for x in x in word_freq[-10:]]
result = "\n".join(output_lines)

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")
outputFile.write(result)
outputFile.close()

sc.stop()
