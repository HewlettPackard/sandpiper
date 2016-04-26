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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{Graph, TripletFields}

object BP {

  def apply(
  graph: Graph[FGVertex, Boolean],
  maxIterations: Int = 50,
  eps: Double = 1e-3): Graph[FGVertex, FGEdge] = {
    // put initial messages on edges, they will be mutated every iteration
    var newGraph = graph.mapTriplets { triplet =>
      new FGEdge(triplet.srcAttr.initMessage(triplet.dstAttr.id),
        triplet.dstAttr.initMessage(triplet.srcAttr.id), false, 0.0f, 0.0f)
    }.cache()
    val numEdges = newGraph.edges.count//foreachPartition(x => {})

    var oldGraph = newGraph
    // main algorithm loop:
    var iter = 0
    var converged = false
    while (iter < maxIterations && !converged) {
      oldGraph = newGraph
      // messages to variables are merged as a product, messages to factors are merged as lists
      val newAggMessages = newGraph.aggregateMessages[List[Message]](
        triplet => {
          triplet.sendToDst(List(triplet.attr.toDst))
          triplet.sendToSrc(List(triplet.attr.toSrc))
        },
        // TODO: extract Merge into the new AggregatedMessage class and use mutable structures
        (m1, m2) => {
          if (m1(0).fromFactor && m2(0).fromFactor) {
            List(Message(m1(0).srcId, m1(0).message.compose(m2(0).message), fromFactor = true))
          } else {
            m1 ++ m2
          }
        },
        TripletFields.EdgeOnly
      )
      newGraph = newGraph.joinVertices(newAggMessages)(
        (id, attr, msg) => attr.processMessage(msg))
        .mapTriplets { triplet =>
          val toSrc = triplet.dstAttr.sendMessage(triplet.attr.toDst)
          val toDst = triplet.srcAttr.sendMessage(triplet.attr.toSrc)
          val diffSrc = toSrc.message.maxDiff(triplet.attr.toSrc.message)
          val diffDst = toDst.message.maxDiff(triplet.attr.toDst.message)
          new FGEdge(toDst, toSrc, diffSrc < eps && diffDst < eps, diffDst, diffSrc)}
        .cache()
      if (iter == 0) {
        newGraph.edges.foreachPartition(x => {})
        converged = false
      } else {
        val numConverged = newGraph.edges.aggregate(0)((res, edge) =>
          if (edge.attr.converged) (res + 1) else res, (res1, res2) =>
            res1 + res2)
        println(numConverged + "/" + numEdges + " edges converged")
        converged = (numConverged == numEdges)
      }
      oldGraph.unpersist(false)
      iter += 1
    }
    println("Iterations: " + iter + "/" + maxIterations +
      ". Converged: " + converged + " with esp=" + eps)
    // TODO: return beliefs as RDD[Beliefs] that can be computed at the end as message product
    newGraph
  }

  def printEdges(graph: Graph[FGVertex, FGEdge]): Unit = {
    graph.edges.collect.foreach(x =>
      println(x.srcId + "-" + x.dstId +
        " toSrc:" + x.attr.toSrc.message.mkString() + " " + x.attr.toSrc.fromFactor +
        " toDst:" + x.attr.toDst.message.mkString() + " " + x.attr.toDst.fromFactor +
        " converged:" + x.attr.converged))

  }

  def printVertices(graph: Graph[FGVertex, FGEdge]): Unit = {
    graph.vertices.collect.foreach { case (vid, vertex) => println(vertex.mkString())}
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 until args.length) {
      println(i + "th par: " + args(i))
    }
    val conf = if (args.length == 4 && args(3) == "local") {
      new SparkConf().setAppName("Belief Propagation Application").setMaster("local")
    } else {
      new SparkConf().setAppName("Belief Propagation Application")
    }
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val file = args(0)
    val numIter = args(1).toInt
    val epsilon = args(2).toDouble
    val graph = Utils.loadLibDAIToFactorGraph(sc, file)
    val beliefs = BP(graph, maxIterations = numIter, eps = epsilon)
    println(graph.vertices.count())
    val calculatedProbabilities = beliefs.vertices.flatMap { case(id, vertex) => vertex match {
      case n: NamedVariable => Seq((n.id, n.belief))
      case _ => Seq.empty[(Long, Variable)]
      }
    }.take(20)
    calculatedProbabilities.foreach { case (id: Long, vr: Variable) => println(id + " " + vr.mkString() )}
  }
}
