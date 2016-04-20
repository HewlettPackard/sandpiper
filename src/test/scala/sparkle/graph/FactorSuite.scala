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

import org.scalatest.FunSuite
import sparkle.util.LocalSparkContext

class FactorSuite extends FunSuite with LocalSparkContext {

//  // TODO: add 3d factor test
//  test("factor marginalize") {
//    /*
//    *       x1    margin over x0
//    *       1 4    5
//    *  x0   2 5    7
//    *       3 6    9
//    *
//    *       6 15 <- margin over x1
//    */
//    val factor = Factor(Array(3, 2), Array(1, 2, 3, 4, 5, 6))
//    val margin1 = factor.marginalize(0)
//    assert(margin1.deep == Array(5.0, 7.0, 9.0).deep,
//      "Marginalization over the first variable is (5, 7, 9)")
//    val margin2 = factor.marginalize(1)
//    assert(margin2.deep == Array(6.0, 15.0).deep,
//      "Marginalization over the second variable is (6, 15)")
//  }
//
//  test("factor product") {
//    /*
//    *       x1    msg to x0   product
//    *       1 4    1           1 4
//    *  x0   2 5    2           4 10
//    *       3 6    3           9 18
//    *
//    *       msg to x1   product
//    *       1           1 8
//    *       2           2 10
//    *                   3 12
//    */
//    val factor = Factor(Array(3, 2), Array(1, 2, 3, 4, 5, 6))
//    val message1 = Variable(Array(1, 2, 3))
//    val product1 = factor.product(message1, 0)
//    assert(product1.cloneValues.deep == Array[Double](1, 4, 9, 4, 10, 18).deep,
//      "Product should be (1, 4, 9, 1, 10, 18)")
//    val message2 = Variable(Array(1, 2))
//    val product2 = factor.product(message2, 1)
//    assert(product2.cloneValues.deep == Array[Double](1, 2, 3, 8, 10, 12).deep,
//      "Product should be (1, 2, 3, 8, 10, 12)")
//  }
//
//  test("marginalize the product") {
//    val factor = Factor(Array(3, 2), Array(1, 2, 3, 4, 5, 6))
//    val message1 = Variable(Array(1, 2, 3))
//    val trueMargin1 = factor.product(message1, 0).marginalize(0)
//    val margin1 = factor.marginalOfProduct(message1, 0)
//    assert(trueMargin1.deep == margin1.deep)
//    val message2 = Variable(Array(1, 2))
//    val trueMargin2 = factor.product(message2, 1).marginalize(1)
//    val margin2 = factor.marginalOfProduct(message2, 1)
//    assert(trueMargin2.deep == margin2.deep)
//  }
//
//  test("variable compose non-log") {
//    val var1 = Variable(Array[Double](1, 2, 3), isLogScale = false)
//    val var2 = Variable(Array[Double](3, 4, 5), isLogScale = false)
//    val compose = var1.compose(var2)
//    assert(compose.cloneValues.deep == Array[Double](3, 8, 15).deep, "Compose must be (3, 8, 15)")
//    val varZero = Variable(Array[Double](0, 4, 5), isLogScale = false)
//    val compose2 = var1.compose(varZero)
//    assert(compose2.cloneValues.deep == Array[Double](-1, 8, 15).deep, "Compose must be (3, -2, 15)")
//    val varMinus1 = Variable(Array[Double](-3, 4, 5), isLogScale = false)
//    val compose3 = var1.compose(varMinus1)
//    assert(compose3.cloneValues.deep == Array[Double](-3, 8, 15).deep, "Compose must be (-3, 8, 15)")
//    val varMinus2 = Variable(Array[Double](-3, 4, 5), isLogScale = false)
//    val compose4 = varMinus1.compose(varMinus2)
//    assert(compose4.cloneValues.deep == Array[Double](0, 16, 25).deep, "Compose must be (0, 16, 25)")
//    val compose5 = varMinus1.compose(varZero)
//    assert(compose5.cloneValues.deep == Array[Double](0, 16, 25).deep, "Compose must be (0, 16, 25)")
//  }
//
//  test("variable decompose non-log") {
//    val var1 = Variable(Array[Double](6, 8, 10), isLogScale = false)
//    val var2 = Variable(Array[Double](3, 4, 5), isLogScale = false)
//    val decompose = var1.decompose(var2)
//    assert(decompose.cloneValues.deep == Array[Double](2, 2, 2).deep, "Compose must be (2, 2, 2)")
//    val varMinus1 = Variable(Array[Double](-3, 4, 5), isLogScale = false)
//    val decompose1 = varMinus1.decompose(var2)
//    assert(decompose1.cloneValues.deep == Array[Double](0, 1, 1).deep, "Compose must be (0, 1, 1)")
//    val varZero = Variable(Array[Double](0, 4, 5), isLogScale = false)
//    val decompose2 = varMinus1.decompose(varZero)
//    assert(decompose2.cloneValues.deep == Array[Double](3, 1, 1).deep, "Compose must be (3, 1, 1)")
//  }
//
//  test("variable compose log") {
//    val var1 = Variable(Array[Double](1, 2, 3), isLogScale = true)
//    val var2 = Variable(Array[Double](3, 4, 5), isLogScale = true)
//    val compose1 = var1.compose(var2)
//    assert(compose1.cloneValues.deep == Array[Double](4, 6, 8).deep, "Compose must be (4, 6, 8)")
//  }
//
//  test("variable compose non-log & log") {
//    val var1 = Variable(Array[Double](1, 2, 3), isLogScale = false)
//    val var2 = Variable(Array[Double](math.log(1), math.log(4), math.log(5)), isLogScale = true)
//    val compose = var1.compose(var2)
//    val eps = 1e-5
//    assert(compose.cloneValues.zip(Array[Double](1, 8, 15)).
//      forall { case (x1: Double, x2: Double) => ((x1 - x2) <= eps) }, "Compose must be (4, 6, 8)")
//  }

  test("test compose normalization norm(X:*Z) == norm(norm(X:*Y):* Z) :/ Y") {
    val x = Variable(Array[Double](0.0, 0.0, 1.0))
    val y = Variable(Array[Double](0.0, 0.4, 0.6))
    val z = Variable(Array[Double](0.4, 0.2, 0.4))
    val xy = x.compose(y)
    println("Belief xy: " + xy.mkString())
    val belief = xy.compose(z)
    println("Belief xyz: " + belief.mkString())
    val xz = x.compose(z)
    val by = belief.decompose(y)
    println(xz.getTrueValue().mkString() + " : " + by.getTrueValue().mkString())
    val bz = belief.decompose(z)
    println(xy.getTrueValue().mkString() + " : " + bz.getTrueValue().mkString())
    val yz = y.compose(z)
    val bx = belief.decompose(x)
    println(yz.getTrueValue().mkString() + " : " + bx.getTrueValue().mkString())
  }

  test("test compose normalization norm(X:*Z) == norm(norm(X:*Y):* Z) :/ Y 2") {
    val x = Variable(Array[Double](4.9E-324, 0.5, 0.5))
    val y = Variable(Array[Double](4.9E-324, 4.9E-324, 1.0))
    val z = Variable(Array[Double](4.9E-324, 0.6, 0.4))
    val xy = x.compose(y)
    val belief = xy.compose(z)
    val xz = x.compose(z)
    val by = belief.decompose(y)
    println(xz.getTrueValue().mkString() + " : " + by.getTrueValue().mkString())
    val bz = belief.decompose(z)
    println(xy.getTrueValue().mkString() + " : " + bz.getTrueValue().mkString())
    val yz = y.compose(z)
    val bx = belief.decompose(x)
    println(yz.getTrueValue().mkString() + " : " + bx.getTrueValue().mkString())
  }

  test("test compose log (X:*Z) == ((X:*Y):* Z) :/ Y 2") {
    println("LOG")
    val x = Variable(Array[Double](4.9E-324, 0.5, 0.5)).log()
    val y = Variable(Array[Double](4.9E-324, 4.9E-324, 1.0)).log()
    val z = Variable(Array[Double](4.9E-324, 0.6, 0.4)).log()
    println("x: " + x.mkString())
    println("y: " + y.mkString())
    println("z: " + z.mkString())
    val xy = x.composeLog(y)
    println("xy: " + xy.mkString())
    val belief = xy.composeLog(z)
    println("belief: " + belief.mkString())

    val xz = x.composeLog(z)
    val by = belief.decomposeLog(y)
    println("xz: " + xz.mkString() + " : " + by.mkString())
    println("xz: " + xz.decodeLog().exp().mkString() + " : " + by.decodeLog().exp().mkString())
    val bz = belief.decomposeLog(z)
    println("xy: " + xy.mkString() + " : " + bz.mkString())
    println("xy: " + xy.decodeLog().exp().mkString() + " : " + bz.decodeLog().exp().mkString())
    val yz = y.composeLog(z)
    val bx = belief.decomposeLog(x)
    println("yz: " + yz.mkString() + " : " + bx.mkString())
    println("yz: " + yz.decodeLog().exp().mkString() + " : " + bx.decodeLog().exp().mkString())
  }


  test("test Factor compose/decompose") {
    val f = Factor(Array(2, 2), Array[Double](0.0, 1.0, 1.0, 0.0))
    val x = Variable(Array[Double](1.0, 0.0))
    val y = Variable(Array[Double](0.0, 1.0))
    val b1 = f.compose(x, 0)
    val b2 = b1.compose(y, 1)
    val bx = b2.decompose(x, 0)
    val by = b2.decompose(y, 1)
    println(b1.mkString())
    println(b2.mkString())
    println(bx.getTrueValue().mkString())
    println(by.getTrueValue().mkString())
  }
}
