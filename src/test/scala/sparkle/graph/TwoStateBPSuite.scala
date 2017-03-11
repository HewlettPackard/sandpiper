/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkle.graph

import org.apache.spark.graphx._
import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class TwoStateBPSuite extends FunSuite with LocalSparkContext {

  test("simple bp test") {
    // test from the lectures EECS course 6.869, Bill Freeman and Antonio Torralba.
    // Chapter 7.3.5 Numerical example.

    withSpark { sc =>
      val vertices = sc.parallelize(Seq(
        (1L, BeliefVertex(Array(1.0, 1.0), Array(0.0, 0.0), 0.0)),
        (2L, BeliefVertex(Array(1.0, 1.0), Array(0.0, 0.0), 0.0)),
        (3L, BeliefVertex(Array(1.0, 1.0), Array(0.0, 0.0), 0.0)),
        (4L, BeliefVertex(Array(1.0, 0.0), Array(0.0, 0.0), 0.0))
      ))
      val edges = sc.parallelize(Seq(
        Edge(1L, 2L, BeliefEdge(Array(Array(1.0, 0.9), Array(0.9, 1.0)), Array(1.0, 1.0), Array(1.0, 1.0))),
        Edge(2L, 3L, BeliefEdge(Array(Array(0.1, 1.0), Array(1.0, 0.1)), Array(1.0, 1.0), Array(1.0, 1.0))),
        Edge(2L, 4L, BeliefEdge(Array(Array(1.0, 0.1), Array(0.1, 1.0)), Array(1.0, 1.0), Array(1.0, 1.0)))
      ))
      val graph = Graph(vertices, edges)
      val bpGraph = TwoStateBP(graph, 2)
      val trueProbabilities = Seq(
        1L -> (1.0 / 2.09 * 1.09, 1.0 / 2.09 * 1.0),
        2L -> (1.0 / 1.1 * 1.0, 1.0 / 1.1 * 0.1),
        3L -> (1.0 / 1.21 * 0.2, 1.0 / 1.21 * 1.01),
        4L -> (1.0, 0.0)).sortBy { case (vid, _) => vid }
      val calculatedProbabilities = bpGraph.vertices.collect().sortBy { case (vid, _) => vid }
      val eps = 10e-5
      calculatedProbabilities.zip(trueProbabilities).foreach {
        case ((_, vertex), (_, (trueP0, trueP1))) =>
          assert(trueP0 - vertex.belief(0) < eps && trueP1 - vertex.belief(1) < eps)
      }
    }

  }

}
