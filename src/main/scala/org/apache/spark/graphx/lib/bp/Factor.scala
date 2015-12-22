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

trait FGVertex {

}

class Factor(varNum: Int, varIds: Array[Int], varNumValues: Array[Int], nonZeroNum: Int, indexAndValues: Array[(Int, Double)]) extends FGVertex {
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
    for (i <- varNumValues.length - 1 to 0) {
      offset = indices(i - 1) + varNumValues(i - 1) * offset
    }
    values(offset)
  }

}

case class Variable() extends FGVertex