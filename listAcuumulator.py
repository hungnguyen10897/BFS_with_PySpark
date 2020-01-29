import pyspark
from pyspark.accumulators import AccumulatorParam

class SetAccumulator(AccumulatorParam):
    def zero(self, element):
        return set()
    
    def addInPlace(self, v1, v2):
        return v1.union(v2)

        