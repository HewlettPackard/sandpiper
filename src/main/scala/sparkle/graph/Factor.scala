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
    if (x == Double.PositiveInfinity || y == Double.PositiveInfinity) Double.PositiveInfinity
    else if (x > 0 && y > 0) Double.PositiveInfinity
    else if (x > 0 && y < 0) x - y
    else if (x < 0 && y > 0) -x + y
    else if (x == Double.NegativeInfinity && y <= 0) -y
    else if (x <= 0 && y == Double.NegativeInfinity) -x
    else x + y
  }
  def decomposeLog(source: Double, x: Double): Double = {
    if (source == Double.PositiveInfinity) Double.PositiveInfinity
    else if (source > 0 && x == Double.NegativeInfinity) -source
    else if (source > 0 && x < 0) source + x
    else if (source < 0 && x > 0) throw new UnsupportedOperationException("Source < 0 && x > 0")
    else if (source == Double.NegativeInfinity) throw new UnsupportedOperationException("Source == -inf")
    else source - x
  }
  def decodeLog(x: Double): Double = {
    if (x > 0) Double.NegativeInfinity
    else x
  }
  def compose(x: Double, y: Double): Double = {
    val dx = 1 / x
    val dy = 1 / y
    if (dx == Double.NegativeInfinity || dy == Double.NegativeInfinity) -0.0
    else if (x == 0 && y == 0) -0.0
    else if (x == 0 && y > 0) -y
    else if (x > 0 && y == 0) -x
    else if (x < 0 && y < 0) -0.0
    else x * y
  }
  def decompose(source: Double, x: Double): Double = {
    if (source == 0 || 1 / source == Double.NegativeInfinity) 0
    else if (source < 0 && x == 0) -source
    else if (source < 0 && x > 0) 0
    else if (source > 0 && x <= 0) throw new UnsupportedOperationException()
    else source / x
  }
  def trueNormalize(array: Array[Double]): Unit = {
    var i = 0
    var sum: Double = 0
    while (i < array.length) {
      sum += math.abs(array(i))
      i += 1
    }
    if (sum == 0) return
    i = 0
    while (i < array.length) {
      val dx = 1 / array(i)
      val abs = math.abs(array(i))
      if (dx != Double.NegativeInfinity) {
        //if (1 - abs < 1e-200) array(i) = if (array(i) > 0) 1.0 else -1.0
        if (abs < 1e-200) array(i) = 0.0
      }
      array(i) = array(i) / sum// if (array(i) == Double.NegativeInfinity) 0 else array(i) / sum
      i += 1
    }
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
      result(i) = FactorMath.compose(values(i), message.state(indexInTargetState))
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
    val result = new Array[Double](states(index))
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      result(indexInTargetState) += FactorMath.decompose(values(i), message.state(indexInTargetState))
    }
    FactorMath.trueNormalize(result)
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

  def mkString(): String = {
    "states: " + states.mkString(" ") + ", values: " + values.mkString(" ")
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
      result(i) = FactorMath.compose(this.values(i), other.values(i))
      i += 1
    }
    FactorMath.trueNormalize(result)
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
      result(i) = FactorMath.decompose(this.values(i), other.values(i))
      i += 1
    }
    FactorMath.trueNormalize(result)
    new Variable(result)
  }

  def composeLog(other: Variable): Variable = {
    require(this.size == other.size)
    val result = new Array[Double](size)
    var i = 0
    while (i < size) {
      result(i) = FactorMath.composeLog(this.values(i), other.values(i))
      i += 1
    }
    new Variable(result)
  }

  def decomposeLog(other: Variable): Variable = {
    require(this.size == other.size)
    val result = new Array[Double](size)
    var i = 0
    while (i < size) {
      result(i) = FactorMath.decomposeLog(this.values(i), other.values(i))
      i += 1
    }
    new Variable(result)
  }

  /**
   * Make string
   *
   * @return string representation
   */
  def mkString(): String = {
    values.mkString(" ")
  }

  def getTrueValue(): Variable = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      if (x(i) < 0) x(i) = 0
      i += 1
    }
    FactorMath.trueNormalize(x)
    val trueValue = new Variable(x)
    trueValue
  }

  def decodeLog(): Variable = {
    val x = values.clone()
    var i = 0
    while (i < x.length) {
      x(i) = FactorMath.decodeLog(x(i))
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

  def log(): this.type = {
    var i = 0
    while (i < values.length) {
      values(i) = math.log(values(i))
      i += 1
    }
    this
  }

  def exp(): this.type = {
    var i = 0
    while (i < values.length) {
      values(i) = math.exp(values(i))
      i += 1
    }
    this
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
