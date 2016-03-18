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

import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class UtilSuite extends FunSuite with LocalSparkContext {

  test("local file read") {
    val (factors, _) = Utils.loadLibDAI("data/factor/graph7.fg")
    val totalNum = factors.length
    val numFactors = factors.count { vertex => vertex match {
      case f: NamedFactor => true
      case _ => false
    }}
    assert(numFactors == 3 && totalNum == 7, "Graph7.fg contains 3 complex factors and 4 variables")
  }

  test("read file into RDD") {
    withSpark { sc =>
      val graph = Utils.loadLibDAIToFactorGraph(sc, "data/factor/graph7.fg")
      val totalNum = graph.vertices.count()
      val numFactors = graph.vertices.map { vertex => vertex._2 match {
          case f: NamedFactor => 1
          case _ => 0
        }}.sum().toLong
      assert(numFactors == 3 && totalNum == 7,
        "Graph7.fg contains 3 complex factors and 4 variables")
    }
  }

  // TODO: make test with reading several files
  test("read several files into RDD") {
    withSpark { sc =>
      val graph = Utils.loadLibDAIToFactorGraph(sc, "data/factor/split")
      val totalNum = graph.vertices.count()
      val numFactors = graph.vertices.map { vertex => vertex._2 match {
        case f: NamedFactor => 1
        case _ => 0
      }}.sum().toLong
      assert(numFactors == 3 && totalNum == 7,
        "Graph7.fg contains 3 complex factors and 4 variables")
    }
  }

}
