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

import org.apache.spark.graphx.Graph

object BP {

  def apply(graph: Graph[FGVertex, Boolean], maxIterations: Int = 50, maxDiff: Double = 1e-3): Graph[FGVertex, Boolean] = {
    // put messages on edges, they will be mutated every iteration
    val bpGraph = graph.mapTriplets { triplet =>
      val srcId = triplet.srcAttr.id
      val dstId = triplet.dstAttr.id
      // find factor vertex on the triplet and get number of values for connected variable
      val toDst = triplet.srcAttr match {
        case srcFactor: NamedFactor => srcFactor.marginalize(dstId)
        case _ => null
      }
      val toSrc =  triplet.dstAttr match {
        case dstFactor: NamedFactor => dstFactor.marginalize(srcId)
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
