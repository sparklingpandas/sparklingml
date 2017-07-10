package com.sparklingpandas.sparklingml

import org.apache.lucene.analysis.Analyzer

/**
 * Abstract trait for Lucene Transformer. An alternative option is to
 * use LuceneTextAnalyzerTransformer from the spark-solr project.
 */
trait LuceneTransformer extends UnaryTransformer[String, Array[String], LuceneTransformer] {

  // Implement this function to construct an analyzer based on the provided settings.
  def buildAnalyzer(): Analyzer

  override def outputDataType: DataType = ArrayType[StringType]

  override def validateInputType(inputType: DataType): Unit = {
    require(inputType.isInstanceOf[StringType],
      s"The input column must be StringType, but got $inputType.")
  }

  override def createTransformFunc: String => Array[String] = {
    val analyzer = buildAnalyzer()
    (input: String) => {
      analyzer.analyze($(inputCol), input)
    }
  }
}
