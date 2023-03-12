# -*- coding: utf-8 -*-
"""
Created on Fri Jan 14 01:03:14 2022

@author: user
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def information(x):
    v = 0;
    if x == 'information':
        v=v+1;
    return (v)

rdd = sc.textFile("file:///SparkCourse/ml-100k/README.md");
rdd1 = rdd.flatMap(lambda x : x.split(' '))
counter = rdd1.countByValue();
print(counter);
