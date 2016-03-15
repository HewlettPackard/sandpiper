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

import java.io.{FileInputStream, InputStream}

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Utils {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)
    val files = sc.binaryFiles("c:/ulanov/tmp")
    println(files.count)
  }

  def loadLibDAIToFactorGraph(sc: SparkContext, path: String): Graph[FGVertex, Boolean] = {
    val files = sc.binaryFiles(path)
    println(files.count())
    val x = sc.binaryFiles(path).map { case (_, stream) =>
        loadLibDAI(stream.open())
    }
    // TODO: refactor y => y, type of edge
    val (vertices, edges) = (x.keys.flatMap(y => y).map(x => (x.id, x)),
      x.values.flatMap(y => y).map(x => Edge(x._1, x._2, true)))
    val graph = Graph(vertices, edges)
    graph
  }

  def loadLibDAI(fileName: String): (Array[FGVertex], Array[(Long, Long)]) = {
    val inputStream = new FileInputStream(fileName)
    loadLibDAI(inputStream)
  }

  def loadLibDAI(stream: InputStream): (Array[FGVertex], Array[(Long, Long)]) = {
    var lines = Source.fromInputStream(stream).getLines()
    // read the number of factors in the file
    lines = lines.dropWhile(l => l.startsWith("#") || l.size < 1)
    val numFactors = lines.next.trim.toInt
    // TODO: check that this structure is OK and size estimation is fair enough
    val factorBuffer = new ArrayBuffer[FGVertex](numFactors * 2)
    val edgeBuffer = new ArrayBuffer[(Long, Long)](numFactors * 10)
    // read factors
    var factorCounter = 0
    // TODO: add factor ids in source format as comment e.g. #222
    while (factorCounter < numFactors) {
      // skip to the next block with factors
      lines = lines.dropWhile(l => !l.startsWith("###"))
      val factorId = lines.next.split(" ")(1).trim.toLong
      lines = lines.dropWhile(_.startsWith("#"))
      val varNum = lines.next.trim.toInt
      lines = lines.dropWhile(_.startsWith("#"))
      val varIds = lines.next.split("\\s+").map(_.toLong)
      lines = lines.dropWhile(_.startsWith("#"))
      val varNumValues = lines.next.split("\\s+").map(_.toInt)
      lines = lines.dropWhile(_.startsWith("#"))
      val nonZeroNum = lines.next.toInt
      val indexAndValues = new Array[(Int, Double)](nonZeroNum)
      var nonZeroCounter = 0
      while (nonZeroCounter < nonZeroNum) {
        lines = lines.dropWhile(_.startsWith("#"))
        val indexAndValue = lines.next.split("\\s+")
        indexAndValues(nonZeroCounter) = (indexAndValue(0).toInt, indexAndValue(1).toDouble)
        nonZeroCounter += 1
      }
      // create Factor vertex
      val factor = NamedFactor(factorId, varIds, varNumValues, nonZeroNum, indexAndValues)
      // create Variable vertex if factor has only one variable and add factor there as a prior
      if (varNum == 1) {
        // TODO: think if beliefs can be added later for the algorithm
        val variable = new NamedVariable(varIds(0),
          belief = Variable.fill(varNumValues(0))(1.0),
          prior = Variable(factor.marginalize(varIds(0))))
        factorBuffer += variable
      } else {
        factorBuffer += factor
        // create edges between Variables and Factor
        for (varId <- varIds) {
          edgeBuffer += new Tuple2(varId, factorId)
        }
      }
      // increment of factor counter
      factorCounter += 1
    }
    (factorBuffer.toArray, edgeBuffer.toArray)
  }
}
