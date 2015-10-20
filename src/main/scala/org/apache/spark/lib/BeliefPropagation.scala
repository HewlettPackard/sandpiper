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

package org.apache.spark.graphx.lib


import java.util.Calendar

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._

import scala.reflect.ClassTag

import org.apache.spark.Logging

object BeliefPropagation extends Logging  {

  // TODO: check if this is still needed and move to apply
  def runWithIterationNumber
  (graph: Graph[BeliefVertex, BeliefEdge],
   maxIterations: Int = Int.MaxValue) : Graph[BeliefVertex, BeliefEdge] =
  {
    val initialMessage = Array(1.0, 1.0)
    val eps = 1e-3
    var maxDiff: Double = 1.0
    var newGraph = graph.mapVertices((vid, vdata) => vertexProgram(0, vid, vdata, initialMessage)).cache()
    var prevGraph: Graph[BeliefVertex, BeliefEdge] = null
    var graphWithNewMessages: Graph[BeliefVertex, BeliefEdge] = null
    var i = 1
    while (i <= maxIterations && maxDiff >= eps) {
      prevGraph = newGraph
      graphWithNewMessages = newGraph.mapTriplets(putMessages).cache()
      graphWithNewMessages.edges.foreachPartition( x => {})
      val newAggMessages = graphWithNewMessages.aggregateMessages[Array[Double]](
        triplet => {
          triplet.sendToDst(triplet.attr.forwardMessage)
          triplet.sendToSrc(triplet.attr.reverseMessage)
        },
        mergeMessages,
        TripletFields.EdgeOnly)
      newGraph = graphWithNewMessages.joinVertices(newAggMessages) { (id, attr, msg) =>
        vertexProgram(i, id, attr, msg) }.cache()
      // TODO: check that edges force vertices to materialize
      newGraph.edges.foreachPartition( x => {})
      prevGraph.vertices.unpersist(false)
      prevGraph.edges.unpersist(false)
      graphWithNewMessages.edges.unpersist(false)
      maxDiff = newGraph.vertices.map { case (vid, vb) => vb.diff }.max()
      println(Calendar.getInstance().getTime() + ": finished " + i + "-th iteration" + " maxDiff:" + maxDiff)
      i += 1
    }
    newGraph
  } // end of apply

  // TODO: remove this after making LOG a parameter
  def putMessages(edge: EdgeTriplet[BeliefVertex, BeliefEdge]):
  BeliefEdge = {
    // copy factor from the edge to the message
    val factor = edge.attr.factor
    val newForwardMessage = Array.ofDim[Double](2)
    val newReverseMessage = Array.ofDim[Double](2)
    val srcBelief = edge.srcAttr.belief
    val dstBelief = edge.dstAttr.belief
    val forwardMessage = Array.ofDim[Double](2)
    val reverseMessage = Array.ofDim[Double](2)
    val maxForward = math.max(edge.attr.forwardMessage(0), edge.attr.forwardMessage(1))
    val maxReverse = math.max(edge.attr.reverseMessage(0), edge.attr.reverseMessage(1))
    forwardMessage(0) = math.exp(edge.attr.forwardMessage(0) - maxForward)
    reverseMessage(0) = math.exp(edge.attr.reverseMessage(0) - maxReverse)
    forwardMessage(1) = math.exp(edge.attr.forwardMessage(1) - maxForward)
    reverseMessage(1) = math.exp(edge.attr.reverseMessage(1) - maxReverse)
    newForwardMessage(0) = math.log(
      (factor(0)(0) * srcBelief(0) / reverseMessage(0) +
        factor(1)(0) * srcBelief(1) / reverseMessage(1)
        ) / (
        factor(0)(0) * srcBelief(0) / reverseMessage(0) +
          factor(1)(0) * srcBelief(1) / reverseMessage(1) +
          factor(0)(1) * srcBelief(0) / reverseMessage(0) +
          factor(1)(1) * srcBelief(1) / reverseMessage(1)
        )
    )
    newReverseMessage(0) = math.log(
      (factor(0)(0) * dstBelief(0) / forwardMessage(0) +
        factor(0)(1) * dstBelief(1) / forwardMessage(1)
        ) / (
        factor(0)(0) * dstBelief(0) / forwardMessage(0) +
          factor(0)(1) * dstBelief(1) / forwardMessage(1) +
          factor(1)(0) * dstBelief(0) / forwardMessage(0) +
          factor(1)(1) * dstBelief(1) / forwardMessage(1)
        )
    )
    newForwardMessage(1) = math.log(
      (factor(0)(1) * srcBelief(0) / reverseMessage(0) +
        factor(1)(1) * srcBelief(1) / reverseMessage(1)
        ) / (
        factor(0)(0) * srcBelief(0) / reverseMessage(0) +
          factor(1)(0) * srcBelief(1) / reverseMessage(1) +
          factor(0)(1) * srcBelief(0) / reverseMessage(0) +
          factor(1)(1) * srcBelief(1) / reverseMessage(1)
        )
    )
    newReverseMessage(1) = math.log(
      (factor(1)(0) * dstBelief(0) / forwardMessage(0) +
        factor(1)(1) * dstBelief(1) / forwardMessage(1)
        ) / (
        factor(0)(0) * dstBelief(0) / forwardMessage(0) +
          factor(0)(1) * dstBelief(1) / forwardMessage(1) +
          factor(1)(0) * dstBelief(0) / forwardMessage(0) +
          factor(1)(1) * dstBelief(1) / forwardMessage(1)
        )
    )
    BeliefEdge(factor, newForwardMessage, newReverseMessage)
  }

  def vertexProgram(niter: Int, id: VertexId,
                    bVertex: BeliefVertex,
                    msgProd: Array[Double]): BeliefVertex = {
    // TODO: refactor computations
    val oldBelief1 = bVertex.belief(0)
    val oldBelief2 = bVertex.belief(1)
    val tmpBelief1 = math.log(bVertex.prior(0)) + msgProd(0)
    val tmpBelief2 = math.log(bVertex.prior(1)) + msgProd(1)
    val maxBelief = math.max(tmpBelief1, tmpBelief2)
    val tmpBelief1_1 = math.exp(tmpBelief1 - maxBelief)
    val tmpBelief1_2 = math.exp(tmpBelief2 - maxBelief)
    val sum = tmpBelief1_1 + tmpBelief1_2
    val newBelief1 = tmpBelief1_1 / sum
    val newBelief2 = tmpBelief1_2 / sum
    val maxDiff = math.max(math.abs(newBelief1 - oldBelief1), math.abs(newBelief2 - oldBelief2))
    BeliefVertex(bVertex.prior, Array(newBelief1, newBelief2), maxDiff)
  }

  def mergeMessages(a: Array[Double], b: Array[Double]): Array[Double] = {
    val res = new Array[Double](2)
    res(0) = a(0) + b(0)
    res(1) = a(1) + b(1)
    res
  }

  def apply[ED: ClassTag]
  (graph: Graph[BeliefVertex, BeliefEdge],
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either): Graph[BeliefVertex, BeliefEdge] = {
    runWithIterationNumber(graph, maxIterations)
  }

  def runUntilConvergence(graph: Graph[BeliefVertex, BeliefEdge], iter: Int):
  Graph[BeliefVertex, BeliefEdge] =
  {
    BeliefPropagation(graph, maxIterations=iter)
  }
}
/**
 *
 * @param prior prior probabilities of each state (default - 2 states)
 * @param belief current probabilities of the state (belief)
 * @param diff difference with the belief on the previous step
 */
case class BeliefVertex(prior: Array[Double], belief: Array[Double], diff: Double)

/**
 *
 * @param factor factor table: rows represent A states, columns - B states
 * @param forwardMessage message from A to B according to the factor table, rows are the states
 * @param reverseMessage message from B to A (reverse in respect of the factor table)
 */
case class BeliefEdge(factor: Array[Array[Double]], forwardMessage: Array[Double], reverseMessage: Array[Double])

