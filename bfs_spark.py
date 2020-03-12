"""
Implementation of BFS using PySpark
"""

import pyspark
from pyspark import SparkContext, SparkConf
from setAccumulator import SetAccumulator

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
    return (parts[0], (parts[1:], 9999, False))

def iteration_process(element, to_visit_ids, distance):
    global to_visit_ids_accu
    global found_target
    
    id = element[0]
    visited = element[1][2]

    if (visited is False) and (id in to_visit_ids):

        print(f"VISITING {id} - {str(visited)}")
        if id == hero_id_target_broadcast.value:
            found_target += 1
        visited = True
        to_visit_ids_accu.add(set(element[1][0]))\
        
        return (id, (element[1][0], distance, True))

    return (id, (element[1][0], distance, visited))

if __name__ == "__main__":

    hero_id_source = '3513'
    hero_id_target = '3759'

    conf = SparkConf().setMaster('local').setAppName('Marvel_Heroes_seperation')
    sc = SparkContext(conf = conf)

    to_visit_ids_accu = sc.accumulator(set(), SetAccumulator())
    found_target =sc.accumulator(0)
    hero_id_target_broadcast = sc.broadcast(hero_id_target)

    lines = sc.textFile("./data/Marvel-Graph.txt")

    #To confirm that some hero_id are repeated as the first id on several lines.
    # rdd1_ = lines.map(lambda x: (x.split()[0], 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], ascending=False)
    # res = rdd1_.take(5)
    # print(res)

    #Inital processing of dataset
    rdd1 = lines.map(initial_process_line).reduceByKey(lambda x,y: (set(x[0]).union(set(y[0])), x[1], x[2]))
    #After this, each item will have type: (hero_id, (connections, distance_from_source, visited)). E.g: ('12',(['14','33'],4,False))

    to_visit_ids_accu.add({hero_id_source})

    distance = 0
    #Iterations
    while True:
        #Take ids of nodes to visit
        to_visit_ids = to_visit_ids_accu.value

        #No more nodes to visit
        if len(to_visit_ids) == 0:
            print(f"No connection between {hero_id_source} and {hero_id_target}.")
            break

        to_visit_ids_accu = sc.accumulator(set(), SetAccumulator())

        print("TO VISIT: ")
        print(to_visit_ids)

        rdd1 = rdd1.map(lambda x: iteration_process(x, to_visit_ids, distance))

        #Without persist() the whole algorithm won't work, not sure why!
        rdd1.persist()

        #Trigger RDD computation
        rdd1.count()
        
        if found_target.value > 0:
            print(f"Seperation between {hero_id_source} and {hero_id_target} is {str(distance)}")
            break

        distance += 1