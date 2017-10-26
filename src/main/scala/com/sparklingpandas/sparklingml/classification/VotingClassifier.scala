package com.sparklingpandas.sparklingml.classification

import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset


trait ClassifierTypeTrait {
  // scalastyle:off structural.type
  type ClassifierType = Classifier[Vector, E, M] forSome {
    type M <: ClassificationModel[Vector, M]
    type E <: Classifier[Vector, E, M]
  }
  // scalastyle:on structural.type
}

class VotingClassifier(override val uid: String)
  extends Classifier[Vector, VotingClassifier, VotingClassifierModel]
    with ClassifierTypeTrait
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("votingclassifier"))

  val classifiers: Param[Array[ClassifierType]] =
    new Param(this, "classifiers", "set of base classifiers")

  /** @group getParam */
  def getClassifiers: Array[ClassifierType] = $(classifiers)

  def setClassifiers(clfs: Array[ClassifierType]): this.type = set(classifiers, clfs)

  override def copy(extra: ParamMap) = ???

  override protected def train(dataset: Dataset[_]) = {
    val estimators = $(classifiers)
    val models = estimators.map { est => est.fit(dataset) }
    val model = new VotingClassifierModel(uid, models).setParent(this)
    copyValues(model)
  }

}

class VotingClassifierModel(
    override val uid: String,
    val models: Array[_ <: ClassificationModel[Vector, _]])
  extends ClassificationModel[Vector, VotingClassifierModel] {

  override def copy(extra: ParamMap) = ???

  override def numClasses = 2

  private def hammerOfTheGods(m: ClassificationModel[Vector, _], features: Vector) = {
    val meth = m.getClass.getMethod("predict", classOf[Vector])
    meth.invoke(m, features).asInstanceOf[Double]
  }

  override protected def predictRaw(features: Vector) = {
    val rawArray = Array.ofDim[Double](numClasses)
    models.foreach { m =>
      val prediction = hammerOfTheGods(m, features)
      val idx = if (prediction > 0.0) 1 else 0
      rawArray(idx) += 1.0
    }
    Vectors.dense(rawArray)
  }

}