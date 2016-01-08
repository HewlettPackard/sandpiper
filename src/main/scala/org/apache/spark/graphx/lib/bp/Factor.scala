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

import org.apache.spark.graphx.{Edge, Graph}

trait FGVertex {
  val id: Long
}

class Factor(
  val id: Long,
  varNum: Int,
  val varIds: Array[Long],
  varNumValues: Array[Int],
  nonZeroNum: Int,
  indexAndValues: Array[(Int, Double)]) extends FGVertex {

  private val values = new Array[Double](varNumValues.product)
  var i = 0
  while (i < indexAndValues.size) {
    val (index, value) = indexAndValues(i)
    values(index) = value
    i += 1
  }

  def value(indices: Seq[Int]): Double = {
    // NB: leftmost index changes the fastest
    // NB: Wikipedia column-major order
    var offset = indices.last
    for (i <- varNumValues.length - 1 to 1 by -1) {
      offset = indices(i - 1) + varNumValues(i - 1) * offset
    }
    values(offset)
  }

  def length(varId: Long): Int = {
    val index = varIds.indexOf(varId)
    if (index == -1) -1 else varNumValues(index)
  }

}

class Variable(val id: Long) extends FGVertex

class Messages(val toDst: Array[Double], val toSrc: Array[Double])

object FactorBP {

  def apply(graph: Graph[FGVertex, Boolean]): Graph[FGVertex, Boolean] = {
    // put messages on edges, they will be mutated every iteration
    val bpGraph = graph.mapTriplets { triplet =>
      val srcId = triplet.srcAttr.id
      val dstId = triplet.dstAttr.id
      // find factor vertex on the triplet and get number of values for connected variable
      val messageSize = triplet.srcAttr match {
        case srcFactor: Factor => srcFactor.length(dstId)
        case _ => triplet.dstAttr match {
          case dstFactor: Factor => dstFactor.length(srcId)
          case _=> 0
        }
      }
      // put initial messages
      new Messages(Array.fill[Double](messageSize)(1.0), Array.fill[Double](messageSize)(1.0))
    }
    // TODO: iterate with bpGraph.mapTriplets (compute and put new messages on edges)

    // TODO: return beliefs as RDD[Beliefs] that can be computed at the end as message product
    graph
  }
}
