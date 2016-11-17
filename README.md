## Loopy Belief Propagation
The project contains an implementation of Loopy Belief Propagation, a popular message passing algorithm for performing inference in probabilistic graphical models. It provides exact inference for graphical models without loops. While inference for graphical models with loops is approximate, in practice it is shown to work well. Our implementation is generic and operates on factor graph representation of graphical models. It handles factors of any order, and variable domains of any size. It is implemented in Apache Spark GraphX, and thus can scale to large scale models. Further, it supports computations in log scale for numerical stability.
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

###References
1. [Belief propagation algorithm](https://en.wikipedia.org/wiki/Belief_propagation)
2. [LibDAI library and file format](https://staff.fnwi.uva.nl/j.m.mooij/libDAI/)
3. Talk by the project authors: [Alexander Ulanov, Manish Marwah "Malicious site detection with large scale belief propagation", Strata+Hadoop 2017](http://conferences.oreilly.com/strata/strata-ca/public/schedule/detail/55701)
