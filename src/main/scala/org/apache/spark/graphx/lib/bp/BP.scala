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

import org.apache.spark.graphx.{TripletFields, Graph}

object BP {

  def apply(graph: Graph[FGVertex, Boolean], maxIterations: Int = 50, eps: Double = 1e-3): Graph[FGVertex, FGEdge] = {
    // put initial messages on edges, they will be mutated every iteration
    var newGraph = graph.mapTriplets { triplet =>
      new FGEdge(triplet.srcAttr.initMessage(triplet.dstAttr.id),
        triplet.dstAttr.initMessage(triplet.srcAttr.id), false)
    }//.cache()

    var oldGraph = newGraph
    // main algorithm loop:
    var iter = 0
    while (iter < maxIterations) {
      // messages to variables are merged as a product, messages to factors are merged as lists
      val newAggMessages = newGraph.aggregateMessages[List[Message]](
        triplet => {
          triplet.sendToDst(List(triplet.attr.toDst))
          triplet.sendToSrc(List(triplet.attr.toSrc))
        },
        // TODO: extract Merge into the new AggregatedMessage class and use mutable structures
        (m1, m2) => {
          if (m1(0).fromFactor && m2(0).fromFactor) {
            List(Message(m1(0).srcId, m1(0).message.compose(m2(0).message), true))
            //List(Message(m1(0).srcId, m1(0).message.sum(m2(0).message), true))
            //List(Message(m1(0).srcId, m1(0).message.product(m2(0).message), true))
          } else {
            m1 ++ m2
          }
        },
        TripletFields.EdgeOnly
      )
      val graphWithNewVertices = newGraph.joinVertices(newAggMessages)(
        (id, attr, msg) => attr.processMessage(msg))//.cache()
      //graphWithNewVertices.edges.foreachPartition(x => {})
      oldGraph = newGraph
      newGraph = graphWithNewVertices.mapTriplets { triplet =>
        val toSrc = triplet.dstAttr.message(triplet.attr.toDst)
        val toDst = triplet.srcAttr.message(triplet.attr.toSrc)
        val diffSrc = toSrc.message.maxDiff(triplet.attr.toSrc.message)
        val diffDst = toDst.message.maxDiff(triplet.attr.toDst.message)
        println(triplet.srcId + "-" + triplet.dstId)
        println("old toSrc: " + triplet.attr.toSrc.message.mkString())
        println("new toSrc: " + toSrc.message.mkString())
        println("diff: "  + diffSrc)
        println("old toDst: " + triplet.attr.toDst.message.mkString())
        println("new toDst: " + toDst.message.mkString())
        println("diff: "  + diffDst)
        // TODO: different scales log and not log compared with eps
        new FGEdge(toDst, toSrc, diffSrc < eps && diffDst < eps)
      }//.cache()
      //newGraph.edges.foreach(x => {})
      printEdges(newGraph)
      //oldGraph.unpersist(false)
      //graphWithNewVertices.unpersist(false)
      iter += 1
    }
    // TODO: iterate with bpGraph.mapTriplets (compute and put new messages on edges)

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
}
