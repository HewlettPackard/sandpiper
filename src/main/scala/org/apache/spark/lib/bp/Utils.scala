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

package org.apache.spark.lib.bp

import scala.io.Source

object Utils {

  def loadLibDAI(path: String): Unit = {
    var lines = Source.fromFile(path).getLines()
    // read the number of factors in the file
    lines = lines.dropWhile(l => l.startsWith("#") || l.size < 1)
    var numFactors = lines.next.toInt
    // read factors
    while (numFactors > 0) {
      // skip to the next block with factors
      lines = lines.dropWhile(l => l.startsWith("#") || l.size < 1)
      val numVars = lines.next.toInt
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
      numFactors -= 1
    }
  }
}
