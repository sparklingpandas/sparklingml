package com.sparklingpandas.sparklingml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

/**
 * Abstract trait for Lucene Transformer. An alternative option is to
 * use LuceneTextAnalyzerTransformer from the spark-solr project.
 */
@DeveloperApi
trait LuceneTransformer extends UnaryTransformer[String, Array[String], LuceneTransformer] {

  // Implement this function to construct an analyzer based on the provided settings.
  def buildAnalyzer(): Analyzer

  override def outputDataType: DataType = ArrayType(StringType)

  override def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[StringType],
      s"The input column must be StringType, but got $inputType.")
  }

  override def createTransformFunc: String => Array[String] = {
    val analyzer = buildAnalyzer()
      (inputText: String) => {
      val inputStream = analyzer.tokenStream($(inputCol), inputText)
      val builder = Array.newBuilder[String]
      val charTermAttr = inputStream.addAttribute(classOf[CharTermAttribute])
      inputStream.reset()
      while (inputStream.incrementToken) builder += charTermAttr.toString
      inputStream.end()
      inputStream.close()
      builder.result()
    }
  }
}
