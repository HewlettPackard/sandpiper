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
  def composeLog(x: Double, y: Double): Double = {
    if (x == Double.NegativeInfinity || y == Double.NegativeInfinity || x == Double.PositiveInfinity || y == Double.PositiveInfinity) throw new UnsupportedOperationException("Negative Inf")
    x + y
  }
  def decomposeLog(source: Double, y: Double): Double = {
    if (source == Double.NegativeInfinity || y == Double.NegativeInfinity) throw new UnsupportedOperationException("Negative Inf")
    source - y
  }
  def logNormalize(x: Array[Double]): Unit = {
    var i = 0
    var sumExp = 0.0
    val max = x.max
    while (i < x.length) {
      sumExp += math.exp(x(i) - max)
      i += 1
    }
    sumExp = math.log(sumExp)
    i = 0
    while (i < x.length) {
      x(i) = x(i) - max - sumExp
      i += 1
    }
  }
  def logSum(x: Double, y: Double): Double = {
    val (a, b) = if (x >= y) (x, y) else (y, x)
    a + math.log1p(math.exp(b - a))
  }
  def logDiff(x: Double, y: Double): Double = {
    val (a, b) = if (x >= y) (x, y) else (y, x)
    a + math.log1p(-math.exp(b - a))
  }

  def composeLogSign(x: Double, y: Double): Double = {
    if (x == Double.PositiveInfinity || y == Double.PositiveInfinity) Double.PositiveInfinity
    else if (x == Double.NegativeInfinity && y == Double.NegativeInfinity) Double.PositiveInfinity
    else if (x > 0 && y > 0) Double.PositiveInfinity
    else if (x > 0 && y < 0) x - y
    else if (x < 0 && y > 0) -x + y
    else if (x == Double.NegativeInfinity && y < 0) -y
    else if (x < 0 && y == Double.NegativeInfinity) -x
    // zero special case
    else if (x == Double.NegativeInfinity && y == 0 && 1 / y > 0) -0.0
    else if (x == 0 && 1 / x > 0 && y == Double.NegativeInfinity) -0.0
    else if (x == Double.NegativeInfinity && y == 0 && 1 / y < 0) Double.PositiveInfinity
    else if (x == 0 && 1 / x < 0 && y == Double.NegativeInfinity) Double.PositiveInfinity
    else if (x < 0 && y == 0 && 1 / y < 0) -x
    else if (x == 0 && 1 / x < 0 && y < 0) -y
    else if (x == 0 && 1 / x < 0 && y == 0 && 1 / y < 0) Double.PositiveInfinity
    else if (x == 0 && 1 / x > 0 && y == 0 && 1 / y < 0) -0.0
    else if (x == 0 && 1 / x < 0 && y == 0 && 1 / y > 0) -0.0
    else x + y
  }
  def decomposeLogSign(source: Double, x: Double): Double = {
    if (source == Double.PositiveInfinity) Double.PositiveInfinity
    else if (source > 0 && x == Double.NegativeInfinity) -source
    else if (source > 0 && x < 0) Double.NegativeInfinity
    else if (source < 0 && x > 0) throw new UnsupportedOperationException("Source < 0 && x > 0")
    else if (source == Double.NegativeInfinity) throw new UnsupportedOperationException("Source == -inf")
    // zero special case
    else if (source == 0 && 1 / source < 0 && x == Double.NegativeInfinity) -source
    else if (source == 0 && 1 / source < 0 && x < 0) Double.NegativeInfinity
    else source - x
  }
  def decodeLogSign(x: Double): Double = {
    if (x > 0) Double.NegativeInfinity
    // zero special case
    else if (x == 0 && 1 / x < 0) Double.NegativeInfinity
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
 * private def value(indices: Seq[Int]): Double = {
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
class Factor private (protected val states: Array[Int], protected val values: Array[Double]) extends Serializable {

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
    val result = new Array[Double](length)
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
    val result = Array.fill[Double](states(index))(Double.NegativeInfinity)
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
  def cloneValues: Array[Double] = {
    // TODO: remove?
    values.clone()
  }

  def exp(): Factor = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      x(i) = math.exp(x(i))
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

  def apply(states: Array[Int], values: Array[Double]): Factor = {
    new Factor(states, values)
  }
}


/**
 * Variable class
 *
 * @param values values
 */
class Variable private (
  protected val values: Array[Double]) extends Serializable {

  // TODO: come up with a single function for elementwise operations given a function
  val size = values.length

  def state(index: Int): Double = values(index)

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
    val result = new Array[Double](size)
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
    val result = new Array[Double](size)
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
    var sum = 0.0
    while (i < x.length) {
      x(i) = math.exp(x(i))
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
      x(i) = math.exp(x(i))
      i += 1
    }
    new Variable(x)
  }

  def maxDiff(other: Variable): Double = {
    require(other.size == this.size, "Variables must have same size")
    var i = 0
    var diff = 0d
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
  def cloneValues: Array[Double] = {
    values.clone()
  }
}

object Variable {

  def apply(values: Array[Double]): Variable = {
    new Variable(values)
  }

  def fill(size: Int, isLogScale: Boolean = false)(elem: => Double): Variable = {
    new Variable(Array.fill[Double](size)(elem))
  }
}
