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

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class BPPerformance extends FunSuite with LocalSparkContext {

  test("bp performance test") {
    withSpark { sc =>
      val vertices: RDD[String] = sc.textFile("data/vertex100.txt")
      val edges: RDD[String] = sc.textFile("data/edge100.txt")
      println( "edges=" + edges.count())
      val vertexRDD = vertices.map { line =>
        val fields = line.split(' ')
        (fields(0).toLong, BeliefVertex(Array(fields(1).toDouble, fields(2).toDouble), Array(0.0, 0.0), 0.0))
      }
      val edgeRDD = edges.map { line =>
        val fields = line.split(' ')
        Edge(fields(0).toLong, fields(1).toLong, BeliefEdge(Array(Array(fields(2).toDouble, fields(4).toDouble), Array(fields(3).toDouble, fields(5).toDouble)), Array(1.0, 1.0), Array(1.0, 1.0)))
      }
      vertexRDD.count
      edgeRDD.count
      val graph = Graph(vertexRDD, edgeRDD)
      val time = System.nanoTime()
      val bpGraph = SimpleBP.runUntilConvergence(graph,50)
      println("Total time: " + (System.nanoTime() - time) / 1e9 + " s." )
      //bpGraph.vertices.foreach { case(vid, belief) => println(vid + ":" + belief.belief(0) + "," + belief.belief(1))}
    }
  }
}
