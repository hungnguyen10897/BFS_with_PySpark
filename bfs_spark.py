"""
Implementation of BFS using PySpark
"""

import pyspark
from pyspark import SparkContext, SparkConf

def getHeroNamesDict():
    hero_names = {}
    with open("./data/Marvel-Names.txt", 'r') as f:
        for line in f:
            if line != "":
                line = line.strip()
                parts = line.split(" ")
                id = parts[0]
                name = ' '.join(parts[1:]).strip("\"")
                hero_names[id] = name
    return hero_names

def initial_process_line(line):
    line = line.strip()
    parts = line.split(' ')
    return (parts[0], parts[1:])

def initial_data_setup(element, hero_id_source):
    color = 'WHITE' if element[0] != hero_id_source else 'BLUE'
    distance = 9999 if element[0] != hero_id_source else 0
    return (element[0],(element[1],distance, color))

def hero_seperation(hero_id_source, hero_id_target):
    
    conf = SparkConf().setMaster('local').setAppName('Marvel_Heroes_seperation')
    sc = SparkContext(conf = conf)

    lines = sc.textFile("./data/Marvel-Graph.txt")

    #To confirm that some hero_id are repeated as the first id on several lines.
    # rdd1_ = lines.map(lambda x: (x.split()[0], 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], ascending=False)
    # res = rdd1_.take(5)
    # print(res)

    #Inital processing of dataset
    rdd1 = lines.map(initial_process_line).reduceByKey(lambda x,y: list(set(x).union(set(y)))).map(lambda x: initial_data_setup(x, hero_id_source))
    #After this, each item will have type: (hero_id, (connections, distance_from_source, color)). E.g: ('12',(['14','33'],4,'WHITE'))

    rdd1.persist()

    #Iterations
    while True:
        



