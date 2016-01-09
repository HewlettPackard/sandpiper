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

package org.apache.spark.graphx.lib.bp

import org.apache.spark.LocalSparkContext
import org.scalatest.FunSuite

class BPSuite  extends FunSuite with LocalSparkContext {

  test ("BP graph test") {
    withSpark { sc =>
      val graph = Utils.loadLibDAIToFactorGraph(sc, "c:/ulanov/dev/belief-propagation/data/factor")
      FactorBP.apply(graph)
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
