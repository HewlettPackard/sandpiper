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

object FactorMath {
  def log(x: Float): Float = math.log(x).toFloat
  def log1p(x: Float): Float = math.log1p(x).toFloat
  def exp(x: Float): Float = math.exp(x).toFloat
  def composeLog(x: Float, y: Float): Float = {
    if (x == Float.NegativeInfinity || y == Float.NegativeInfinity || x == Float.PositiveInfinity || y == Float.PositiveInfinity) throw new UnsupportedOperationException("Negative Inf")
    x + y
  }
  def decomposeLog(source: Float, y: Float): Float = {
    if (source == Float.NegativeInfinity || y == Float.NegativeInfinity) throw new UnsupportedOperationException("Negative Inf")
    source - y
  }
  def logNormalize(x: Array[Float]): Unit = {
    var i = 0
    var sumExp = 0.0f
    val max = x.max
    while (i < x.length) {
      sumExp += exp(x(i) - max)
      i += 1
    }
    sumExp = log(sumExp)
    i = 0
    while (i < x.length) {
      x(i) = x(i) - max - sumExp
      i += 1
    }
  }
  def logSum(x: Float, y: Float): Float = {
    val (a, b) = if (x >= y) (x, y) else (y, x)
    a + log1p(exp(b - a))
  }
  def logDiff(x: Float, y: Float): Float = {
    val (a, b) = if (x >= y) (x, y) else (y, x)
    a + log1p(-exp(b - a))
  }

  def composeLogSign(x: Float, y: Float): Float = {
    if (x == Float.PositiveInfinity || y == Float.PositiveInfinity) Float.PositiveInfinity
    else if (x == Float.NegativeInfinity && y == Float.NegativeInfinity) Float.PositiveInfinity
    else if (x > 0 && y > 0) Float.PositiveInfinity
    else if (x > 0 && y < 0) x - y
    else if (x < 0 && y > 0) -x + y
    else if (x == Float.NegativeInfinity && y < 0) -y
    else if (x < 0 && y == Float.NegativeInfinity) -x
    // zero special case
    else if (x == Float.NegativeInfinity && y == 0 && 1 / y > 0) -0.0f
    else if (x == 0 && 1 / x > 0 && y == Float.NegativeInfinity) -0.0f
    else if (x == Float.NegativeInfinity && y == 0 && 1 / y < 0) Float.PositiveInfinity
    else if (x == 0 && 1 / x < 0 && y == Float.NegativeInfinity) Float.PositiveInfinity
    else if (x < 0 && y == 0 && 1 / y < 0) -x
    else if (x == 0 && 1 / x < 0 && y < 0) -y
    else if (x == 0 && 1 / x < 0 && y == 0 && 1 / y < 0) Float.PositiveInfinity
    else if (x == 0 && 1 / x > 0 && y == 0 && 1 / y < 0) -0.0f
    else if (x == 0 && 1 / x < 0 && y == 0 && 1 / y > 0) -0.0f
    else x + y
  }
  def decomposeLogSign(source: Float, x: Float): Float = {
    if (source == Float.PositiveInfinity) Float.PositiveInfinity
    else if (source > 0 && x == Float.NegativeInfinity) -source
    else if (source > 0 && x < 0) Float.NegativeInfinity
    else if (source < 0 && x > 0) throw new UnsupportedOperationException("Source < 0 && x > 0")
    else if (source == Float.NegativeInfinity) throw new UnsupportedOperationException("Source == -inf")
    // zero special case
    else if (source == 0 && 1 / source < 0 && x == Float.NegativeInfinity) -source
    else if (source == 0 && 1 / source < 0 && x < 0) Float.NegativeInfinity
    else source - x
  }
  def decodeLogSign(x: Float): Float = {
    if (x > 0) Float.NegativeInfinity
    // zero special case
    else if (x == 0 && 1 / x < 0) Float.NegativeInfinity
    else x
  }
}

/**
 *
 * Representation of a factor
 * Example how to loop through all variables
 *  for (i <- 0 until values.length) {
 *      var product: Int = 1
 *     for (dim <- 0 until varNumValues.length - 1) {
 *     val dimValue = (i / product) % varNumValues(dim)
 *        product *= varNumValues(dim)
 *        print(dimValue)
 *      }
 *      println(i / product)
 *    }
 *
 * Accessing a value by index
 * private def value(indices: Seq[Int]): Float = {
 *   // NB: leftmost index changes the fastest
 *   // NB: Wikipedia column-major order
 *   var offset = indices.last
 *   for (i <- states.length - 1 to 1 by -1) {
 *     offset = indices(i - 1) + states(i - 1) * offset
 *   }
 *   values(offset)
 * }
 *
 * @param states number of states
 * @param values values in vector format
 */
class Factor private (protected val states: Array[Int], protected val values: Array[Float]) extends Serializable {

  /**
   * Total length of the factor in vector representation
   *
   * @return length
   */
  val length: Int = values.length

  /**
   * Number of states of a variable at index
   *
   * @param index index
   * @return number of states
   */
  def length(index: Int): Int = {
    states(index)
  }

  /**
    * Returns a composition of factor and a message.
    * Handles zero values the same as message composition
    *
    * @param message
    * @param index
    * @return
    */
  def compose(message: Variable, index: Int): Factor = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.size,
      "Number of states for variable and message must be equal")
    val result = new Array[Float](length)
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      result(i) = FactorMath.composeLog(values(i), message.state(indexInTargetState))
    }
    Factor(states, result)
  }

  /**
    * Returns decomposition of factor and a message
    * Handles zero values the same as message decomposition
    *
    * @param message
    * @param index
    * @return
    */
  def decompose(message: Variable, index: Int): Variable = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.size,
      "Number of states for variable and message must be equal")
    val result = Array.fill[Float](states(index))(Float.NegativeInfinity)
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      val decomposed = FactorMath.decomposeLog(values(i), message.state(indexInTargetState))
      val logSum = FactorMath.logSum(result(indexInTargetState), decomposed)
      result(indexInTargetState) = logSum
    }
    Variable(result)
  }

  /**
   * Clone values
   *
   * @return values
   */
  def cloneValues: Array[Float] = {
    // TODO: remove?
    values.clone()
  }

  def exp(): Factor = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      x(i) = FactorMath.exp(x(i))
      i += 1
    }
    Factor(this.states, x)
  }

  def mkString(): String = {
    "states: " + states.mkString(" ") + ", values (exp): " + exp().values.mkString(" ")
  }
}

/**
 * Fabric for factor creation
 */
object Factor {

  def apply(states: Array[Int], values: Array[Float]): Factor = {
    new Factor(states, values)
  }
}


/**
 * Variable class
 *
 * @param values values
 */
class Variable private (
  protected val values: Array[Float]) extends Serializable {

  // TODO: come up with a single function for elementwise operations given a function
  val size = values.length

  def state(index: Int): Float = values(index)

  /**
    * Compose two variables
    * Composition. If zero state is present in one of the messages,
    * while the other contains non-reversed value,
    * then the result state equals to the non-zero state with reversed sign.
    * If the other contains reversed value or zero, then the result will be zero.
    *
    * @param other other variable
    * @return aggregation result
    */
  def compose(other: Variable): Variable = {
    require(this.size == other.size)
    val result = new Array[Float](size)
    var i = 0
    while (i < size) {
      result(i) = FactorMath.composeLog(this.values(i), other.values(i))
      i += 1
    }
    FactorMath.logNormalize(result)
    new Variable(result)
  }

  /**
    * Decompose two variables
    * Decomposition. If zero state is present in the second message,
    * then the resulting state will be either zero if state in the first
    * message is not reversed or zero, or minus state of the first message overwise.
    *
    * @param other
    * @return
    */
  def decompose(other: Variable): Variable = {
    require(this.size == other.size)
    val result = new Array[Float](size)
    var i = 0
    while (i < size) {
      result(i) = FactorMath.decomposeLog(this.values(i), other.values(i))
      i += 1
    }
    FactorMath.logNormalize(result)
    new Variable(result)
  }

  /**
   * Make string
   *
   * @return string representation
   */
  def mkString(norm: Boolean = false): String = {
    if (norm) expNorm().values.mkString(" ") else exp().values.mkString(" ")
  }

  def decodeLog(): Variable = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      x(i) = FactorMath.decodeLogSign(x(i))
      i += 1
    }
    new Variable(x)
  }

  def expNorm(): Variable = {
    val x = values.clone()
    var i = 0
    var sum = 0.0f
    while (i < x.length) {
      x(i) = FactorMath.exp(x(i))
      sum += x(i)
      i += 1
    }
    i = 0
    while (i < x.length) {
      x(i) = x(i) / sum
      i += 1
    }
    new Variable(x)
  }

  def exp(): Variable = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      x(i) = FactorMath.exp(x(i))
      i += 1
    }
    new Variable(x)
  }

  def maxDiff(other: Variable): Float = {
    require(other.size == this.size, "Variables must have same size")
    var i = 0
    var diff = 0f
    while (i < values.length) {
      val d = math.abs(values(i) - other.values(i))
      diff = if (d > diff) d else diff
      i += 1
    }
    diff
  }

  /**
   * Clone values
   *
   * @return values
   */
  def cloneValues: Array[Float] = {
    values.clone()
  }
}

object Variable {

  def apply(values: Array[Float]): Variable = {
    new Variable(values)
  }

  def fill(size: Int, isLogScale: Boolean = false)(elem: => Float): Variable = {
    new Variable(Array.fill[Float](size)(elem))
  }
}
