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

import java.io.{FileInputStream, InputStream, InputStreamReader, BufferedReader}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source

object Utils {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)
    val files = sc.binaryFiles("c:/ulanov/tmp")
    println(files.count)
  }

  def loadLibDAIToRDD(sc: SparkContext, path: String): RDD[FGVertex] = {
    val files = sc.binaryFiles(path)
    println(files.count())
    sc.binaryFiles(path).flatMap { case (_, stream) =>
        loadLibDAI(stream.open())
    }
  }

  def loadLibDAI(fileName: String): Seq[FGVertex] = {
    val inputStream = new FileInputStream(fileName)
    loadLibDAI(inputStream)
  }

  def loadLibDAI(stream: InputStream): Seq[FGVertex] = {
    var lines = Source.fromInputStream(stream).getLines()
    // read the number of factors in the file
    lines = lines.dropWhile(l => l.startsWith("#") || l.size < 1)
    val numFactors = lines.next.trim.toInt
    val factors = new Array[FGVertex](numFactors)
    // read factors
    var factorCounter = 0
    while (factorCounter < numFactors) {
      // skip to the next block with factors
      lines = lines.dropWhile(l => l.startsWith("#") || l.size < 1)
      val numVars = lines.next.trim.toInt
      lines = lines.dropWhile(_.startsWith("#"))
      val varIds = lines.next.split("\\s+").map(_.toInt)
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
      factors(factorCounter) = new Factor(numVars, varIds, varNumValues, nonZeroNum, indexAndValues)
      factorCounter += 1
    }
    factors
  }
}
