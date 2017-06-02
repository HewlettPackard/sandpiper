## Loopy Belief Propagation
The project contains an implementation of Loopy Belief Propagation,
a popular message passing algorithm for performing inference in probabilistic graphical models.
It provides exact inference for graphical models without loops.
While inference for graphical models with loops is approximate, in practice it is shown to work well.
Our implementation is generic and operates on factor graph representation of graphical models.
It handles factors of any order, and variable domains of any size.
In addition, we provide specialized implementation for pairwise factors.
The algorithm is implemented with Apache Spark GraphX, and thus can scale to large scale models.
Further, it supports computations in log scale for numerical stability.

### Building
Clone and build with Maven:
````
git clone https://github.com/HewlettPackard/sandpiper.git
mvn package
````
The library will appear in the `target` folder. By default it is compiled against Scala 2.11.8
and Spark 1.6.1. If you need a different Scala/Spark versions, please modify `pom.xml`.

### BP for Factor Graph
We use factor graph format derived from libDAI with small modification
([example](https://github.com/HewlettPackard/sandpiper/blob/master/data/factor/graph7.fg)):
````
# number of factors in the file
7

# factor id preceded with 3 hashes (unique, must not intersect with variable name/id)
### 5
# number of vars
1
# name of vars
1
# number of values of vars
2
# number of non-zero entries in factor table
2
# non-zero factor table entries
0  1
1  1

````
Data parallel loading requires the factors to be partitioned across multiple files.

***Code***
````
import sparkle.graph._
// Load graph from the input path with factor files
val graph = Utils.loadLibDAIToFactorGraph(sc, inputPath)
// Run Belief Propagation on the graph with maximum iterations "maxIterations" and epsilon "epsilon"
val beliefs = BP(graph, maxIterations, epsilon)
````

***Command line***

````
./spark-submit --master spark://MASTER:PORT --jars belief-propagation.jar --class sparkle.graph.BP INPUT_PATH OUTPUT_PATH N_ITERATIONS EPSILON
````
### BP for pairwise factors
We use two file format. Each line of the first file contains
variable id and its prior delimited with space
([example](https://github.com/HewlettPackard/sandpiper/blob/master/data/vertex4.txt)):
````
1 1.0 1.0
2 1.0 0.0
...
````
The second file contains pairwise factors with first variable id, second variable id,
and the factor in column-major format delimited with space
([example](https://github.com/HewlettPackard/sandpiper/blob/master/data/edge4.txt)):
 ````
 1 2 1.0 0.9 0.9 1.0 
 2 3 0.1 1.0 1.0 0.1 
...
 ````
No additional effort is required for enabling data parallel loading since each variable and
factor are stored on separate lines and can automatically be processed by Spark in parallel.

***Code***
````
import sparkle.graph._
// Load graph from two input files
val graph = PairwiseBP.loadPairwiseGraph(sc, variableFile, factorFile)
// Run Belief Propagation on the graph with maximum iterations "maxIterations" and epsilon "epsilon"
val beliefs = PairwiseBP(graph, maxIterations, epsilon)
````

***Command line***

````
./spark-submit --master spark://MASTER:PORT --jars belief-propagation.jar --class sparkle.graph.PairwiseBP VARIABLES_FILE FACTORS_FILE OUTPUT_PATH N_ITERATIONS EPSILON
````


### References
1. Belief propagation [algorithm](https://en.wikipedia.org/wiki/Belief_propagation)
1. Yedidia, J.S.; Freeman, W.T.; Weiss, Y., "Understanding Belief Propagation and Its Generalizations" in Exploring Artificial    Intelligence in the New Millennium, Lakemeyer, G. and Nebel, B., Eds., ISBN: 1-55860-811-7, chapter 8, pp. 239-236, Morgan Kaufmann Publishers, January 2003. ([pdf](http://www.merl.com/publications/docs/TR2001-22.pdf))
   [An excellent description of BP]
2. LibDAI file [format](https://staff.fnwi.uva.nl/j.m.mooij/libDAI/) is used with explicit factor ids: [example](https://github.com/HewlettPackard/sandpiper/blob/master/data/factor/graph7.fg).
3. [Talk](http://conferences.oreilly.com/strata/strata-ca/public/schedule/detail/55701) by the project authors: Alexander Ulanov, Manish Marwah "Malicious site detection with large scale belief propagation", *Strata+Hadoop*, March 2017 ([slides](https://github.com/HewlettPackard/sandpiper/SandpiperStrata2017))

### How to contribute
Contributors are required to sign the [Corporate Contributor License Agreement](https://github.com/HewlettPackard/sandpiper/blob/master/HPE_CCLA.txt)
