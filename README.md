# BFS_with_PySpark
Implementation of Breadth First Search in Spark (Python flavor) to find the degree of seperation between nodes in a graph.
<br/><br/>

## Data
**Marvel-Graph.txt**: contains graph data (in form of ids). Each line starts with a hero id and follows by ids of heroes that occured together in a film/comic book. 

**Marvel-Names.txt**: mapping from hero id to actual hero name. For reference only, not used in the program.
<br/><br/>

## General idea
The objective is to frame the BFS algorithm into parallelized computing problem to solve with Spark. In this problem, each hero (node) has a set of connections (hero ids of those appearing in the same comic book/ movie). Heroes with ids on the same line have seperation of 1, represented by an edge in a graph. The degree of seperation is the number of edges between two nodes (if they are connected at all).

This is a bit easier since the distance between all nodes (length of an edge) is always 1.

After initially transforming the original data to a form ready for processing, each iteration processes some of the nodes:
* check if this is the target node (target hero id)
* calculate the distance from the source node (each iteration makes seperation 1 degree larger)
* set its connections to be processed on the next iteration (if it has not been yet)
<br/><br/>

## Implementation features

### 1. SetAccumulator: **_to_visit_ids_accu_**

Accumulator is a special type of variable used in Spark so that other computing nodes can report back to the driver. A custom accumulator is implemented so that after processing a node, a set of node ids is returned to driver, for the next iteration.

### 2. Normal Accumulator: **_found_target_**

Used to signal when the target hero id is found.
