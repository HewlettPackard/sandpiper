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
1. Belief propagation [algorithm](https://en.wikipedia.org/wiki/Belief_propagation)
1. Yedidia, J.S.; Freeman, W.T.; Weiss, Y., "Understanding Belief Propagation and Its Generalizations" in Exploring Artificial    Intelligence in the New Millennium, Lakemeyer, G. and Nebel, B., Eds., ISBN: 1-55860-811-7, chapter 8, pp. 239-236, Morgan Kaufmann Publishers, January 2003. ([pdf](http://www.merl.com/publications/docs/TR2001-22.pdf))
   [An excellent description of BP]
2. LibDAI file [format](https://staff.fnwi.uva.nl/j.m.mooij/libDAI/)
3. [Talk](http://conferences.oreilly.com/strata/strata-ca/public/schedule/detail/55701) by the project authors: Alexander Ulanov, Manish Marwah "Malicious site detection with large scale belief propagation", *Strata+Hadoop*, March 2017

###How to contribute
Contributors are required to sign the [Corporate Contributor License Agreement](https://github.com/HewlettPackard/sandpiper/blob/master/HPE_CCLA.txt)
