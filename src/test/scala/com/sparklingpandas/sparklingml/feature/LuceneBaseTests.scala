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

package com.sparklingpandas.sparklingml.feature

import org.apache.spark.ml.param._

import org.apache.lucene.analysis.Analyzer

import org.scalatest._

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import com.sparklingpandas.sparklingml.param._

case class InputData(input: String)

abstract class LuceneTransformerTest[T <: LuceneTransformer[_]] extends
    FunSuite with DataFrameSuiteBase with Matchers {
  val transformer: T

  test("verify that the transformer runs") {
    import spark.implicits._
    val input = spark.createDataset(
      List(InputData("hi"), InputData("boo"), InputData("boop")))
    transformer.setInputCol("input")
    val result = transformer.transform(input).collect()
    result.size shouldBe 3
  }
}

abstract class LuceneStopwordTransformerTest[T <: LuceneTransformer[_]] extends
    LuceneTransformerTest[T] {
  test("verify stopword is dropped, nothing else") {
    import spark.implicits._
    val input = spark.createDataset(
      List(InputData("hi"), InputData("boo"), InputData("boop")))
    val thst = transformer.asInstanceOf[HasStopwords]
    thst.set(thst.stopwords, Array("boo"))
    thst.setStopwords(Array("boop"))
    transformer.asInstanceOf[T].setInputCol("input")
    val result = transformer.transform(input).collect()
    result.size shouldBe 3
    result(2).getSeq(1) shouldBe empty
  }
}
