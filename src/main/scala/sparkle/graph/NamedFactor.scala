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

trait FGVertex extends Serializable {
  val id: Long
  def mkString(): String
  def processMessage(aggMessage: List[Message]): FGVertex
  def sendMessage(incomingMessage: Message): Message
  def initMessage(varId: Long): Message
}

class NamedFactor(val id: Long, val variables: Array[Long], val factor: Factor, val belief: Factor)
  extends FGVertex {
  /**
    * Returns variable index in the values array by its ID
    *
    * @param varId variable ID
    * @return index
    */
  private def varIndexById(varId: Long): Int = {
    val index = variables.indexOf(varId)
    require(index >= 0, "Variable not found in factor")
    index
  }

  /**
    * Number of values for a variable
    *
    * @param varId variable id
    * @return number of values
    */
  def length(varId: Long): Int = {
    val index = varIndexById(varId)
    factor.length(index)
  }

  def mkString(): String = {
    "id: " + id.toString() + ", factor: " + factor.mkString() + ", belief: " + belief.mkString()
  }

  override def processMessage(aggMessage: List[Message]): FGVertex = {
    assert(aggMessage.length > 1)
    var newBelief = factor
    for(message <- aggMessage) {
      val index = varIndexById(message.srcId)
      newBelief = newBelief.compose(message.message, index)
    }
    NamedFactor(id, variables, factor, newBelief)
  }

  override def sendMessage(oldMessage: Message): Message = {
    val index = varIndexById(oldMessage.srcId)
    val newMessage = Variable(belief.decompose(oldMessage.message, index))
    Message(this.id, Variable(newMessage.cloneValues), fromFactor = true)
  }

  override def initMessage(varId: Long): Message = {
    Message(this.id, Variable.fill(this.length(varId))(1.0), fromFactor = true)
  }
}

object NamedFactor {

  def apply(id: Long, variables: Array[Long], factor: Factor, belief: Factor): NamedFactor = {
    new NamedFactor(id, variables, factor, belief)
  }

  /**
    * Create factor from the libDAI description
    *
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
    new NamedFactor(id, variables, Factor(states, values), belief = Factor(states.clone(), values.clone()))
  }

  def apply(factor: NamedFactor): NamedFactor = {
    // TODO: implement this
    new NamedFactor(factor.id, factor.variables, factor.factor, factor.belief)
  }
}

case class NamedVariable(val id: Long, val belief: Variable, val prior: Variable) extends FGVertex {
  override def processMessage(aggMessage: List[Message]): FGVertex = {
    assert(aggMessage.length == 1)
    val newBelief = prior.compose(aggMessage(0).message)
    NamedVariable(id, newBelief, prior)
  }

  override def sendMessage(oldMessage: Message): Message = {
    val newMessage = belief.decompose(oldMessage.message)
    Message(this.id, newMessage, fromFactor = false)
  }


  override def initMessage(varId: Long): Message = {
    Message(this.id, Variable.fill(this.belief.size)(1.0), fromFactor = false)
  }

  def mkString(): String = {
    "id: " + id + ", belief: " + belief.mkString() + ", prior: " + prior.mkString()
  }
}

// TODO: reuse NamedVariable class

case class Message(val srcId: Long, val message: Variable, val fromFactor: Boolean)

class FGEdge(val toDst: Message, val toSrc: Message, val converged: Boolean)
