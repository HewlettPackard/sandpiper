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

import org.apache.spark.LocalSparkContext
import org.scalatest.FunSuite

class FactorSuite extends FunSuite with LocalSparkContext {

  // TODO: add 3d factor test
  test("marginalize") {
    val factor = Factor(Array(3, 2), Array(1, 2, 3, 4, 5, 6))
    val margin1 = factor.marginalize(0)
    assert(margin1.deep == Array(5.0, 7.0, 9.0).deep,
      "Marginalization over the first variable is (5, 7, 9)")
    val margin2 = factor.marginalize(1)
    assert(margin2.deep == Array(6.0, 15.0).deep,
      "Marginalization over the second variable is (6, 15)")
  }

//  test("product") {
//
//  }

}
