import pyspark
from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam

def process(e):
    global div_3s
    if e % 3 == 0:
        print("#############" + str(e))
        div_3s.add({e})
    return (e,1)

class SetAccumulator(AccumulatorParam):
    def zero(self, element):
        return set()
    
    def addInPlace(self, v1, v2):
        return v1.union(v2)

def a():
    global div_3s
    sc = SparkContext()
    div_3s = sc.accumulator(set(), accum_param = SetAccumulator())
    div_3s.add({1})

    rdd1 = sc.parallelize(list(range(10)))

    rdd2 = rdd1.map(process)
    print(rdd2.collect())
    print(div_3s.value)

if __name__ == "__main__":
    div_3s = {}
    a()

