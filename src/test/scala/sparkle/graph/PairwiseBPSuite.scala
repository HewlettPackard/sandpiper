package sparkle.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class PairwiseBPSuite  extends FunSuite with LocalSparkContext {

  test("Pairwise BP test") {
    // test from the lectures EECS course 6.869, Bill Freeman and Antonio Torralba.
    // Chapter 7.3.5 Numerical example.

    withSpark { sc =>
      val vertices: RDD[(Long, PVertex)] = sc.parallelize(Seq(
        (1L, PVertex(Variable(Array(0.0, 0.0)), Variable(Array(1.0, 1.0).map(math.log)))),
        (2L, PVertex(Variable(Array(0.0, 0.0)), Variable(Array(1.0, 1.0).map(math.log)))),
        (3L, PVertex(Variable(Array(0.0, 0.0)), Variable(Array(1.0, 1.0).map(math.log)))),
        (4L, PVertex(Variable(Array(0.0, 0.0)), Variable(Array(1.0, 0.0).map(math.log)))))
      )
      val edges = sc.parallelize(Seq(
        Edge(1L, 2L, PEdge(Factor(Array(2, 2), Array(1.0, 0.9, 0.9, 1.0).map(math.log)), Variable(Array(0.0, 0.0)), Variable(Array(0.0, 0.0)))),
        Edge(2L, 3L, PEdge(Factor(Array(2, 2), Array(0.1, 1.0, 1.0, 0.1).map(math.log)), Variable(Array(0.0, 0.0)), Variable(Array(0.0, 0.0)))),
        Edge(2L, 4L, PEdge(Factor(Array(2, 2), Array(1.0, 0.1, 0.1, 1.0).map(math.log)), Variable(Array(0.0, 0.0)), Variable(Array(0.0, 0.0))))
      ))
      val graph = Graph(vertices, edges)
      val bpGraph = PairwiseBP(graph)
      val trueProbabilities = Seq(
        1L -> (1.0 / 2.09 * 1.09, 1.0 / 2.09 * 1.0),
        2L -> (1.0 / 1.1 * 1.0, 1.0 / 1.1 * 0.1),
        3L -> (1.0 / 1.21 * 0.2, 1.0 / 1.21 * 1.01),
        4L -> (1.0, 0.0)).sortBy { case (vid, _) => vid }
      val calculatedProbabilities = bpGraph.vertices.collect().sortBy { case (vid, _) => vid }
      val eps = 10e-5
      calculatedProbabilities.zip(trueProbabilities).foreach {
        case ((_, vertex), (_, (trueP0, trueP1))) =>
          assert(trueP0 - vertex.belief.exp().cloneValues(0) < eps && trueP1 - vertex.belief.exp().cloneValues(1) < eps)
      }
    }

  }

  test("Pariwise BP test with file") {
    withSpark { sc =>
      val graph = PairwiseBP.loadPairwiseGraph(sc, "data/vertex4.txt", "data/edge4.txt")
      val bpGraph = PairwiseBP(graph)
      val trueProbabilities = Seq(
        1L -> (1.0 / 2.09 * 1.09, 1.0 / 2.09 * 1.0),
        2L -> (1.0 / 1.1 * 1.0, 1.0 / 1.1 * 0.1),
        3L -> (1.0 / 1.21 * 0.2, 1.0 / 1.21 * 1.01),
        4L -> (1.0, 0.0)).sortBy { case (vid, _) => vid }
      val calculatedProbabilities = bpGraph.vertices.collect().sortBy { case (vid, _) => vid }
      val eps = 10e-5
      calculatedProbabilities.zip(trueProbabilities).foreach {
        case ((_, vertex), (_, (trueP0, trueP1))) =>
          assert(trueP0 - vertex.belief.exp().cloneValues(0) < eps && trueP1 - vertex.belief.exp().cloneValues(1) < eps)
      }
    }
  }
}
