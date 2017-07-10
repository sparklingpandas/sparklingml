package com.sparklingpandas.sparklingml.feature

import org.apache.spark.annotation.DeveloperApi

import org.apache.lucene.analysis.Analyzer

@DeveloperApi
object LuceneAnalyzerGenerators {
  def generate() = {
    import org.reflections.Reflections
    import collection.JavaConverters._
    import scala.reflect.runtime.universe._
    val reflections = new Reflections("org.apache.lucene");
    val analyzers = reflections.getSubTypesOf(classOf[org.apache.lucene.analysis.Analyzer])
    val rm = scala.reflect.runtime.currentMirror
    val generated = analyzers.asScala.map{ cls =>
      val constructors = rm.classSymbol(cls).toType.members.collect{
        case m: MethodSymbol if m.isConstructor && m.isPublic => m }
      // Since this isn't built with -parameters by default :(
      // we'd need a local version built with it to auto generate
      // the code here with the right parameters.
      // https://docs.oracle.com/javase/tutorial/reflect/member/methodparameterreflection.html
      // For now we could dump the class names and go from their
      // or we could play a game of pin the field on the constructor.
      // local build sounds like the best plan, lets do that l8r
    }
  }
}
