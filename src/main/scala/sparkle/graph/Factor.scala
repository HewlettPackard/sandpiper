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
 * @param states number of states
 * @param values values in vector format
 */
class Factor private (protected val states: Array[Int], protected val values: Array[Double]) extends Serializable {

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
 *
   * @return length
   */
  def length: Int = {
    values.length
  }

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
   * Marginalize factor by a variable
 *
   * @param index index of a variable
   * @return marginal
   */
  def marginalize(index: Int): Array[Double] = {
    require(index >= 0 && index < states.length, "Index must be non-zero && within shape")
    val result = new Array[Double](states(index))
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      result(indexInTargetState) += values(i)
    }
    result
  }

  /**
   * Operation of a factor and a message
 *
   * @param message message to a variable
   * @param index variable index
   * @return new factor
   */
  protected def operation(
  message: Variable,
  index: Int, op: (Double, Double) => Double): Factor = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.size,
      "Number of states for variable and message must be equal")
    val result = new Array[Double](length)
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      result(i) = op(values(i), message.state(indexInTargetState))
    }
    Factor(states, result)
  }

  /**
   * Product of a factor and a message
 *
   * @param message message to a variable
   * @param index variable index
   * @return new factor
   */
  def product(message: Variable, index: Int): Factor = {
    operation(message, index, (x, y) => x * y)
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
      def f = (x: Double, y: Double) => {
        if (x == 0 && y > 0) -y
        else if (x > 0 && y == 0) -x
        else if (x < 0 && y < 0) 0
        else x * y
      }
      result(i) = f(values(i), message.state(indexInTargetState))
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
  def decompose(message: Variable, index: Int): Array[Double] = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.size,
      "Number of states for variable and message must be equal")
    val result = new Array[Double](states(index))
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      def f = (x: Double, y: Double) => {
        if (x == 0) 0
        else if (x < 0 && y == 0) -x
        else if (x < 0 && y > 0) 0
        else if (x > 0 && y <= 0) throw new UnsupportedOperationException()
        else x / y
      }
      result(indexInTargetState) += f(values(i), message.state(indexInTargetState))
    }
    result
  }

  /**
   * Division of a factor and a message
 *
   * @param message message to a variable
   * @param index variable index
   * @return new factor
   */
  def division(message: Variable, index: Int): Factor = {
    operation(message, index, (x, y) => x / y)
  }

  /**
   * Marginal of a factor and a message operation
 *
   * @param message message to a variable
   * @param index index of a variable
   * @return marginal
   */
  def operationAndMarginal(
  message: Variable,
  index: Int,
  op: (Double, Double) => Double): Array[Double] = {
    require(index >= 0, "Index must be non-zero")
    require(states(index) == message.size,
      "Number of states for variable and message must be equal")
    val result = new Array[Double](states(index))
    val product = states.slice(0, index).product
    for (i <- 0 until values.length) {
      val indexInTargetState = (i / product) % states(index)
      result(indexInTargetState) += op(values(i), message.state(indexInTargetState))
    }
    result
  }


  /**
   * Marginal of a product of a factor and a message
 *
   * @param message message to a variable
   * @param index index of a variable
   * @return marginal
   */
  def marginalOfProduct(message: Variable, index: Int): Array[Double] = {
    operationAndMarginal(message, index, (x, y) => x * y)
  }

  /**
   * Division of a product of a factor and a message
 *
   * @param message message to a variable
   * @param index index of a variable
   * @return marginal
   */
  def marginalOfDivision(message: Variable, index: Int): Array[Double] = {
    operationAndMarginal(message, index, (x, y) => x / y)
  }

  /**
   * Clone values
 *
   * @return values
   */
  def cloneValues: Array[Double] = {
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
  protected val values: Array[Double],
  val isLogScale: Boolean = false) extends Serializable {

  // TODO: come up with a single function for elementwise operations given a function
  val size = values.length

  def state(index: Int): Double = values(index)

  /**
   * Operation on two variables
   *
   * @param other variable
   * @return multiplication result
   */
  protected def operation(other: Variable, op: (Double, Double) => Double): Variable = {
    require(this.size == other.size)
    val result = new Array[Double](size)
    var i = 0
    while (i < size) {
      result(i) = op(this.values(i), other.values(i))
      i += 1
    }
    new Variable(result)
  }

  /**
    * Returns the result of composition or decomposition of two messages.
    * Messages can be either normal or log-scale. The type of the result
    * is the same as the first message.
    * Handling zero values:
    * 1) Composition. If zero state is present in one of the messages,
    * while the other contains non-reversed value,
    * then the result state equals to the non-zero state with reversed sign.
    * If the other contains reversed value or zero, then the result will be zero.
    * 2) Decomposition. If zero state is present in the second message,
    * then the resulting state will be either zero if state in the first
    * message is not reversed or zero, or minus state of the first message overwise.
    * 3) Reversed sign for normal type is negative, for log-scale it is positive.
    *
    * @param other variable
    * @param compose compose or decompose
    * @return multiplication result
    */
  protected def ops(other: Variable, compose: Boolean): Variable = {
    require(this.size == other.size)
    val result = new Array[Double](size)
    var i = 0
    def f: (Double, Double) => Double =
      (this.isLogScale, other.isLogScale, compose) match {
      case (true, true, true) =>
        (x: Double, y: Double) => x + y
      case (false, true, true) => (x: Double, y: Double) => x * math.exp(y)
      case (false, false, true) =>
        (x: Double, y: Double) => {
          if (x == 0 && y > 0) -y
          else if (x > 0 && y == 0) -x
          else if (x < 0 && y < 0) 0
          else x * y
        }
      case (true, false, true) => (x: Double, y: Double) => x + math.log(y)
      case (true, true, false) => (x: Double, y: Double) => x - y
      case (false, true, false) => (x: Double, y: Double) => x / math.exp(y)
      case (false, false, false) =>
        (x: Double, y: Double) => {
          if (x == 0) 0
          else if (x < 0 && y == 0) -x
          else if (x < 0 && y > 0) 0
          else if (x > 0 && y <= 0) throw new UnsupportedOperationException()
          else x / y
        }
      case (true, false, false) => (x: Double, y: Double) => x - math.log(y)
    }
    while (i < size) {
        result(i) = f(this.values(i), other.values(i))
      i += 1
    }
    new Variable(result, this.isLogScale)
  }

  /**
    * Compose two variables, i.e. sum if both are log-scale, product otherwise
    *
    * @param other other variable
    * @return aggregation result
    */
  def compose(other: Variable): Variable = {
    ops(other, compose = true)
  }

  /**
    * Decompose two variables, i.e. subtract is both are log-scale, divide otherwise
    *
    * @param other
    * @return
    */
  def decompose(other: Variable): Variable = {
    ops(other, compose = false)
  }

  /**
   * Make string
   *
   * @return string representation
   */
  def mkString(): String = {
    values.mkString(" ")
  }

  def normalize(): Unit = {
    var i = 0
    var sum: Double = 0
    while (i < values.length) {
      if (values(i) > 0) sum += values(i) else values(i) = 0
      i += 1
    }
    if (sum == 0) return
    i = 0
    while (i < values.length) {
      values(i) = values(i) / sum
      i += 1
    }
  }

  def log(): Unit = {
    var i = 0
    while (i < values.length) {
      values(i) = math.log(values(i))
      i += 1
    }
  }

  def maxDiff(other: Variable): Double = {
    require(other.size == this.size, "Variables must have same size")
    var i = 0
    var diff = 0d
    while (i < values.length) {
      val d = values(i) - other.values(i)
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

  def apply(values: Array[Double], isLogScale: Boolean = false): Variable = {
    new Variable(values, isLogScale)
  }

  def fill(size: Int, isLogScale: Boolean = false)(elem: => Double): Variable = {
    new Variable(Array.fill[Double](size)(elem), isLogScale)
  }
}
