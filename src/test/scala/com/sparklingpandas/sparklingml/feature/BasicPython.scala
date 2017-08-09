package com.sparklingpandas.sparklingml.feature

import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._

import org.scalatest._

import com.holdenkarau.spark.testing.DataFrameSuiteBase

class StrLenPlusKPythonSuite extends FunSuite with DataFrameSuiteBase with Matchers {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  test("verify that the transformer runs") {
    import spark.implicits._
    val transformer = new StrLenPlusKPython()
    transformer.setK(1)
    val input = spark.createDataset(
      List(InputData("hi"), InputData("boo"), InputData("boop")))
    transformer.setInputCol("input")
    transformer.setOutputCol("output")
    val result = transformer.transform(input).collect()
    result.size shouldBe 3
    result(0)(0) shouldBe "hi"
    result(0)(1) shouldBe 3
    result(1)(0) shouldBe "boo"
    result(1)(1) shouldBe 4
  }

}
