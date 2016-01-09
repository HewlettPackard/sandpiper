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

  private def value(indices: Seq[Int]): Double = {
    // NB: leftmost index changes the fastest
    // NB: Wikipedia column-major order
    var offset = indices.last
    for (i <- varNumValues.length - 1 to 1 by -1) {
      offset = indices(i - 1) + varNumValues(i - 1) * offset
    }
    values(offset)
  }

  /**
   * Returns variable index in the values array by its ID
   * @param varId variable ID
   * @return index
   */
  private def varIndexById(varId: Long): Int = varIds.indexOf(varId)

  /**
   * Number of values for a variable
   * @param varId variable id
   * @return number of values
   */
  def length(varId: Long): Int = {
    val index = varIndexById(varId)
    if (index == -1) -1 else varNumValues(index)
  }

  def marginalize(varId: Long): Array[Double] = {
    val index = varIndexById(varId)
    require(index >= 0, "Index must be non-zero")
    // K-dimensional marginalization
    val result = new Array[Double](varNumValues(index))
    for (i <- 0 until values.length) {
      var product: Int = 1
      for (dim <- 0 until varNumValues.length - 1) {
        val dimValue = (i / product) % varNumValues(dim)
        product *= varNumValues(dim)
        print(dimValue)
      }
      println(i / product)
    }
    result
  }

}

class Variable(val id: Long) extends FGVertex

class Messages(val toDst: Array[Double], val toSrc: Array[Double])

object FactorBP {

  def apply(graph: Graph[FGVertex, Boolean], maxIterations: Int = 50, maxDiff: Double = 1e-3): Graph[FGVertex, Boolean] = {
    // put messages on edges, they will be mutated every iteration
    val bpGraph = graph.mapTriplets { triplet =>
      val srcId = triplet.srcAttr.id
      val dstId = triplet.dstAttr.id
      // find factor vertex on the triplet and get number of values for connected variable
      val toDst = triplet.srcAttr match {
        case srcFactor: Factor => srcFactor.marginalize(dstId)
        case _ => null
        }
      val toSrc =  triplet.dstAttr match {
        case dstFactor: Factor => dstFactor.marginalize(srcId)
        case _ => null
      }
      // put initial messages
      new Messages(if (toDst != null) toDst else Array.fill[Double](toSrc.length)(1.0),
        if (toSrc != null) toSrc else Array.fill[Double](toDst.length)(1.0))
    }
    bpGraph.edges.collect.foreach(x =>
      println(x.srcId + "-" + x.dstId +
        " toSrc:"  + x.attr.toSrc.mkString(" ") + " toDst:" + x.attr.toDst.mkString(" ")))
    // compute beliefs:

    // TODO: iterate with bpGraph.mapTriplets (compute and put new messages on edges)

    // TODO: return beliefs as RDD[Beliefs] that can be computed at the end as message product
    graph
  }
}
