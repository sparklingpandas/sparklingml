package com.sparklingpandas.sparklingml.feature

import scala.collection.JavaConverters._

import org.apache.lucene.analysis.CharArraySet

object LuceneHelpers {
  /**
   * Convert a provided Array of strings into a CharArraySet.
   */
  def wordstoCharArraySet(input: Array[String], ignoreCase: Boolean): CharArraySet = {
    new CharArraySet(input.toList.asJava, ignoreCase)
  }
}
