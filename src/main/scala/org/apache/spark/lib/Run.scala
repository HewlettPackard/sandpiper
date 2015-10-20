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

package org.apache.spark.lib

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.graphx.lib.{BeliefPropagation, BeliefEdge, BeliefVertex}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Run {

  def main(args: Array[String]): Unit = {
    val conf = if (args.length == 4 && args(3) == "local") {
      new SparkConf().setAppName("Belief Propagation Application").setMaster("local")
    } else {
      new SparkConf().setAppName("Belief Propagation Application")
    }
    val sc = new SparkContext(conf)
    val vertices: RDD[String] = sc.textFile(args(0))
    val edges: RDD[String] = sc.textFile(args(1))
    val maxIterations = args(2).toInt
    println( "edges=" + edges.count())
    val vertexRDD = vertices.map { line =>
      val fields = line.split(' ')
      (fields(0).toLong, BeliefVertex(Array(fields(1).toDouble, fields(2).toDouble), Array(0.0, 0.0), 0.0))
    }
    val edgeRDD = edges.map { line =>
      val fields = line.split(' ')
      Edge(fields(0).toLong, fields(1).toLong, BeliefEdge(Array(Array(fields(2).toDouble, fields(4).toDouble), Array(fields(3).toDouble, fields(5).toDouble)), Array(1.0, 1.0), Array(1.0, 1.0)))
    }
    val graph = Graph(vertexRDD, edgeRDD)
    val time = System.nanoTime()
    BeliefPropagation.runUntilConvergence(graph, maxIterations)
    println("Total time: " + (System.nanoTime() - time) / 1e9 + " s." )
  }
}
