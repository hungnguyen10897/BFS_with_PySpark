"""
Implementation of BFS using PySpark
"""

import pyspark
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('Marvel_Heroes_seperation')
sc = SparkContext(conf = conf)

