/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
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

import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class BeliefPropagationSuite extends FunSuite with LocalSparkContext {

  test ("BP graph test") {
    withSpark { sc =>
      val graph = Utils.loadLibDAIToFactorGraph(sc,
        "data/factor/graph7.fg")
      val bpGraph = BeliefPropagation.apply(graph, maxIterations = 60, eps = 1e-3)
      val trueProbabilities = Map(
        1L -> (1.0 / 2.09 * 1.09, 1.0 / 2.09 * 1.0),
        2L -> (1.0 / 1.1 * 1.0, 1.0 / 1.1 * 0.1),
        3L -> (1.0 / 1.21 * 0.2, 1.0 / 1.21 * 1.01),
        4L -> (1.0, 0.0))
      val calculatedProbabilities = bpGraph.vertices.flatMap { case(id, vertex) => vertex match {
        case n: NamedVariable => Seq((n.id, n.belief.exp()))
        case _ => Seq.empty[(Long, Variable)]
        }
      }.collect()
      val eps = 10e-3
      calculatedProbabilities.foreach { case (id, belief) =>
        assert(belief.state(0) - trueProbabilities(id)._1 < eps &&
          belief.state(1) - trueProbabilities(id)._2 < eps)
      }
    }
  }

// test ("index") {
//  val values = Array[Double](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
//  val varNumValues = Array[Int](2, 3, 2)
//   for (i <- 0 until values.length) {
//     var product: Int = 1
//     for (dim <- 0 until varNumValues.length - 1) {
//       val dimValue = (i / product) % varNumValues(dim)
//       product *= varNumValues(dim)
//       print(dimValue)
//     }
//     println(i / product)
//   }
// }

}
