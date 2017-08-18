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

package com.sparklingpandas.sparklingml.param

import java.io.PrintWriter

import scala.reflect.ClassTag
import scala.xml.Utility

import com.sparklingpandas.sparklingml.CodeGenerator

/**
 * Code generator for shared params (sharedParams.scala). Run with
 * {{{
 *   build/sbt "runMain com.sparklingpandas.sparklingml.param.SharedParamsCodeGen"
 * }}}.
 *
 * Based on the same param generators in Spark, but with extra params.
 */
private[sparklingpandas] object SharedParamsCodeGen extends CodeGenerator {

  def main(args: Array[String]): Unit = {
    val params = Seq(
      // SparklingML Params
      ParamDesc[Boolean]("stopwordCase", "If the case should be considered when filtering stopwords", Some("false")),
      ParamDesc[Array[String]]("stopwords",
        "Stopwords to be filtered. Default value depends on underlying transformer"),
      // Spark Params
      ParamDesc[String]("featuresCol", "features column name", Some("\"features\"")),
      ParamDesc[String]("labelCol", "label column name", Some("\"label\"")),
      ParamDesc[String]("inputCol", "input column name"),
      ParamDesc[Array[String]]("inputCols", "input column names"),
      ParamDesc[String]("outputCol", "output column name", Some("uid + \"__output\"")))

    val code = genSharedParams(params)
    val file = "src/main/scala/com/sparklingpandas/sparklingml/param/sharedParams.scala"
    val writer = new PrintWriter(file)
    writer.write(code)
    writer.close()
  }

  /** Description of a param. */
  private case class ParamDesc[T: ClassTag](
      name: String,
      doc: String,
      defaultValueStr: Option[String] = None,
      isValid: String = "",
      finalMethods: Boolean = true,
      finalFields: Boolean = true,
      isExpertParam: Boolean = false) {

    require(name.matches("[a-z][a-zA-Z0-9]*"), s"Param name $name is invalid.")
    require(doc.nonEmpty) // TODO: more rigorous on doc
    val c = implicitly[ClassTag[T]].runtimeClass

    def paramTypeName: String = {
      paramTypeNameFromClass(c)
    }

    def valueTypeName: String = {
      valueTypeNameFromClass(c)
    }
  }

  private def paramTypeNameFromClass(c: Class[_]): String = {
    c match {
      case _ if c == classOf[Int] => "IntParam"
      case _ if c == classOf[Long] => "LongParam"
      case _ if c == classOf[Float] => "FloatParam"
      case _ if c == classOf[Double] => "DoubleParam"
      case _ if c == classOf[Boolean] => "BooleanParam"
      case _ if c.isArray && c.getComponentType == classOf[String] => s"StringArrayParam"
      case _ if c.isArray && c.getComponentType == classOf[Double] => s"DoubleArrayParam"
      case _ => s"Param[${getTypeString(c)}]"
    }
  }

  private def valueTypeNameFromClass(c: Class[_]): String = {
    getTypeString(c)
  }

  private def getTypeString(c: Class[_]): String = {
    c match {
      case _ if c == classOf[Int] => "Int"
      case _ if c == classOf[Long] => "Long"
      case _ if c == classOf[Float] => "Float"
      case _ if c == classOf[Double] => "Double"
      case _ if c == classOf[Boolean] => "Boolean"
      case _ if c == classOf[String] => "String"
      case _ if c.isArray => s"Array[${getTypeString(c.getComponentType)}]"
    }
  }


  /** Generates the HasParam trait code for the input param. */
  private def genHasParamTrait(param: ParamDesc[_]): String = {
    val name = param.name
    val Name = name(0).toUpper +: name.substring(1)
    val Param = param.paramTypeName
    val T = param.valueTypeName
    val doc = param.doc
    val defaultValue = param.defaultValueStr
    val defaultValueDoc = defaultValue.map { v =>
      s" (default: $v)"
    }.getOrElse("")
    val setDefault = defaultValue.map { v =>
      s"""
         |  setDefault($name, $v)
         |""".stripMargin
    }.getOrElse("")
    val isValid = if (param.isValid != "") {
      ", " + param.isValid
    } else {
      ""
    }
    val groupStr = if (param.isExpertParam) {
      Array("expertParam", "expertGetParam")
    } else {
      Array("param", "getParam")
    }
    val methodStr = if (param.finalMethods) {
      "final def"
    } else {
      "def"
    }
    val fieldStr = if (param.finalFields) {
      "final val"
    } else {
      "val"
    }

    val htmlCompliantDoc = Utility.escape(doc)

    s"""
      |/**
      | * Trait for shared param $name$defaultValueDoc.
      | */
      |trait Has$Name extends Params {
      |
      |  /**
      |   * Param for $htmlCompliantDoc.
      |   * @group ${groupStr(0)}
      |   */
      |  $fieldStr $name: $Param = new $Param(this, "$name", "$doc"$isValid)
      |$setDefault
      |  /** @group ${groupStr(1)} */
      |  $methodStr get$Name: $T = $$($name)
      |
      |  $methodStr set$Name(value: $T): this.type = set(this.$name, value)
      |}
      |""".stripMargin
  }

  /** Generates Scala source code for the input params with header. */
  private def genSharedParams(params: Seq[ParamDesc[_]]): String = {
    val header = s"""${scalaLicenseHeader}
        |
        |package com.sparklingpandas.sparklingml.param
        |
        |import org.apache.spark.ml.param._
        |
        |// DO NOT MODIFY THIS FILE!
        |// It was generated by SharedParamsCodeGen.
        |
        |// scalastyle:off
        |""".stripMargin

    val footer = "// scalastyle:on\n"

    val traits = params.map(genHasParamTrait).mkString

    header + traits + footer
  }
}
