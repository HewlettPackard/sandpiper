## Loopy Belief Propagation
This library contains the implementation of Loopy Belief Propagation on factor graphs for Apache Spark GraphX.

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
