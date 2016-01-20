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

class Factor (val states: Array[Int], private val values: Array[Double]) {

  private def value(indices: Seq[Int]): Double = {
    // NB: leftmost index changes the fastest
    // NB: Wikipedia column-major order
    var offset = indices.last
    for (i <- states.length - 1 to 1 by -1) {
      offset = indices(i - 1) + states(i - 1) * offset
    }
    values(offset)
  }

  /**
   * Total length of the factor in vector representation
   * @return length
   */
  def length: Int = {
    values.length
  }

  /**
   * Number of states of a variable at index
   * @param index index
   * @return number of states
   */
  def length(index: Int): Int = {
    states(index)
  }

  // TODO: call eliminate?
  def marginalize(index: Int): Array[Double] = {
    require(index >= 0 && index < states.length, "Index must be non-zero && within shape")
    val result = new Array[Double](states(index))
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetDimension = (i / product) % states(index)
      result(indexInTargetDimension) += values(i)
    }
    result
  }

  // TODO: do in-place or product & marginalize at once
  def product(message: Array[Double], index: Int): Unit = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.length,
      "Number of states for variable and message must be equal")
    val result = new Array[Double](length)
//    for (i <- 0 until values.length) {
//      var product: Int = 1
//      for (dim <- 0 until varNumValues.length - 1) {
//        val dimValue = (i / product) % varNumValues(dim)
//        product *= varNumValues(dim)
//        print(dimValue)
//      }
//      println(i / product)
//    }
    result
  }
}

/**
 * Fabric for factor creation
 */
object Factor {

  def apply(states: Array[Int], values: Array[Double]): Factor = {
    new Factor(states, values)
  }
}

trait FGVertex {
  val id: Long
}

class NamedFactor(val id: Long, private val variables: Array[Long], val factor: Factor) 
  extends FGVertex {
  /**
   * Returns variable index in the values array by its ID
   * @param varId variable ID
   * @return index
   */
  private def varIndexById(varId: Long): Int = variables.indexOf(varId)

  /**
   * Number of values for a variable
   * @param varId variable id
   * @return number of values
   */
  def length(varId: Long): Int = {
    val index = varIndexById(varId)
    if (index == -1) -1 else factor.length(index)
  }

  // TODO: call eliminate?
  def marginalize(varId: Long): Array[Double] = {
    val index = varIndexById(varId)
    require(index >= 0, "Index must be non-zero")
    factor.marginalize(index)
  }
}

object NamedFactor {
  /**
   * Create factor from the libDAI description
   * @param id unique id
   * @param variables ids of variables
   * @param states num of variables states
   * @param nonZeroNum num of nonzero values
   * @param indexAndValues index and values
   * @return factor
   */
  def apply(
             id: Long,
             variables: Array[Long],
             states: Array[Int],
             nonZeroNum: Int,
             indexAndValues: Array[(Int, Double)]): NamedFactor = {
    val values = new Array[Double](states.product)
    var i = 0
    while (i < indexAndValues.size) {
      val (index, value) = indexAndValues(i)
      values(index) = value
      i += 1
    }
    new NamedFactor(id, variables, Factor(states, values))
  }
}

class Variable(val id: Long) extends FGVertex

class Messages(val toDst: Array[Double], val toSrc: Array[Double])
