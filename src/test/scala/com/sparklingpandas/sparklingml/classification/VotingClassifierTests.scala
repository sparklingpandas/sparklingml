package com.sparklingpandas.sparklingml.classification

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

import org.apache.spark.ml.classification.{DecisionTreeClassifier, LinearSVC, LogisticRegression, NaiveBayes}

class VotingClassifierTests extends FunSuite with DataFrameSuiteBase {


  test("basic operation") {

    val df = spark.read.format("libsvm")
      .load("data/sample_binary_classification_data.txt")

    val dt = new DecisionTreeClassifier()
    val lr = new LogisticRegression()
    val svc = new LinearSVC()
    val vc = new VotingClassifier().setClassifiers(Array(lr, dt, svc))
    val vcm = vc.fit(df)

    vcm.transform(df).show

  }

}
