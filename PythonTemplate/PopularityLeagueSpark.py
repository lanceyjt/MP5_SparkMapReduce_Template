#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
lines_lst = lines.collect()

leagueIds = sc.textFile(sys.argv[2], 1)

#TODO
leagueIds_lst = leagueIds.collect()
leagueIds_lst_int = [int(x) for x in leagueIds_lst]

page_freq = {}
for line in lines_lst:
  #TODO
  line = line.strip()
  _, dest = line.split(":")
  dests = [x.strip() for x in dest.split(' ') if x.strip()!='']
  for d in dests:
    page_freq[d] = page_freq.get(d,0)+1

page_freq_lst = list(page_freq.items())
league_freq_lst = [x for x in page_freq_lst if int(x[0]) in leagueIds_lst_int]

league_freq_lst.sort(reverse=False, key=lambda x: x[1])
diff = [league_freq_lst[i][1] - league_freq_lst[i-1][1] for i in range(1, len(league_freq_lst))]
ranks = [0]
for i in range(len(diff)):
    current_rank = ranks[-1]
    if diff[i] > 0:
        ranks.append(len(ranks))
    else:
        ranks.append(current_rank)

pages = [x[0] for x in league_freq_lst]
page_rank = list(zip(pages, ranks))
page_rank.sort(reverse=True, key=lambda x: x[0])


output = open(sys.argv[3], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for x in page_rank:
    output.write('%s\t%s\n' % (x[0],x[1]))
sc.stop()

