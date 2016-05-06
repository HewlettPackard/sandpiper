# Loopy Belief Propagation
### Description

### Factor graph representation

###Examples

**API**

````
import sparkle.graph._
// Load graph from the inputPath
val graph = Utils.loadLibDAIToFactorGraph(sc, inputPath)
// Run Belief Propagation on the graph with maximum iterations "maxIterations" and epsilon "epsilon"
val beliefs = BeliefPropagation(graph, maxIterations, epsilon)
````

**Spark submit**

````
./spark-submit --master spark://MASTER:PORT --jars belief-propagation.jar --class sparkle.graph.BP INPUT_PATH OUTPUT_PATH N_ITERATIONS EPSILON
````
