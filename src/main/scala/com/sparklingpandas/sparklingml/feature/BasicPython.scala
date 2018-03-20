package com.sparklingpandas.sparklingml.feature

import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._


import com.sparklingpandas.sparklingml.util.python.PythonTransformer

class NltkPosPython(override val uid: String) extends PythonTransformer {

  def this() = this(Identifiable.randomUID("StrLenPlusKPython"))

  override val pythonFunctionName = "nltkpos"
  override protected def outputDataType = DoubleType
  override protected def validateInputType(inputType: DataType): Unit = {
    if (inputType != StringType) {
      throw new IllegalArgumentException(
        s"Expected input type StringType instead found ${inputType}")
    }
  }

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  def miniSerializeParams() = ""
}


class StrLenPlusKPython(override val uid: String) extends PythonTransformer {

  final val k: IntParam = new IntParam(this, "k", "number to add to strlen")

  /** @group getParam */
  final def getK: Int = $(k)

  final def setK(value: Int): this.type = set(this.k, value)

  def this() = this(Identifiable.randomUID("StrLenPlusKPython"))

  override val pythonFunctionName = "strlenplusk"
  override protected def outputDataType = IntegerType
  override protected def validateInputType(inputType: DataType): Unit = {
    if (inputType != StringType) {
      throw new IllegalArgumentException(
        s"Expected input type StringType instead found ${inputType}")
    }
  }

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  def miniSerializeParams() = {
    "[" + $(k) + "]"
  }
}

class SpacyTokenizePython(override val uid: String) extends PythonTransformer {

  final val lang = new Param[String](this, "lang", "language for tokenization")

  /** @group getParam */
  final def getLang: String = $(lang)

  final def setLang(value: String): this.type = set(this.lang, value)

  def this() = this(Identifiable.randomUID("SpacyTokenizePython"))

  override val pythonFunctionName = "spacytokenize"
  override protected def outputDataType = ArrayType(StringType)
  override protected def validateInputType(inputType: DataType): Unit = {
    if (inputType != StringType) {
      throw new IllegalArgumentException(
        s"Expected input type StringType instead found ${inputType}")
    }
  }

  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  def miniSerializeParams() = {
    "[\"" + $(lang) + "\"]"
  }
}
