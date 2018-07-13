from __future__ import unicode_literals

from pyspark import keyword_only
from pyspark.ml import Model
from pyspark.ml.param import *
from pyspark.ml.param.shared import *
from pyspark.ml.util import *
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.functions import (PandasUDFType, UserDefinedFunction,
                                   pandas_udf)

from sparklingml.transformation_functions import *

# Most of the Python models are wrappers of JavaModels, and we will need those
# as well. For now this simple example shows how to expose a simple Python only
# UDF. Look in Spark for exposing Java examples.


@ignore_unicode_prefix
class StrLenPlusKTransformer(Model, HasInputCol, HasOutputCol):
    """
    strLenPlusK takes one parameter it is k and returns
    the string length plus k. This is intended to illustrate how
    to make a Python stage and not for actual use.
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
    >>> df = spark.createDataFrame([("hi",), ("boo",)], ["values"])
    >>> tr = StrLenPlusKTransformer(inputCol="values", outputCol="c", k=2)
    >>> tr.getK()
    2
    >>> tr.transform(df).head().c
    4
    >>> tr.setK(1)
    StrLenPlusKTransformer_...
    >>> tr.transform(df).head().c
    3
    """

    # We need a parameter to configure k
    k = Param(Params._dummy(),
              "k", "amount to add to str len",
              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, k=None, inputCol=None, outputCol=None):
        super(StrLenPlusKTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, k=None, inputCol=None, outputCol=None):
        """
        setParams(self, k=None, inputCol=None, outputCol=None):
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def getK(self):
        """
        Gets the value of K or its default value.
        """
        return self.getOrDefault(self.k)

    def _transform(self, dataset):
        func = StrLenPlusK.func(self.getK())
        ret_type = StrLenPlusK.returnType()
        udf = UserDefinedFunction(func, ret_type)
        return dataset.withColumn(
            self.getOutputCol(), udf(self.getInputCol())
        )


@ignore_unicode_prefix
class SpacyTokenizeTransformer(Model, HasInputCol, HasOutputCol):
    """
    Tokenize the provided input using Spacy.
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
    >>> df = spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
    >>> tr = SpacyTokenizeTransformer(inputCol="vals", outputCol="c")
    >>> str(tr.getLang())
    'en'
    >>> tr.transform(df).head().c
    [u'hi', u'boo']
    """

    # We need a parameter to configure k
    lang = Param(Params._dummy(),
                 "lang", "language",
                 typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, lang="en", inputCol=None, outputCol=None):
        super(SpacyTokenizeTransformer, self).__init__()
        self._setDefault(lang="en")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, lang="en", inputCol=None, outputCol=None):
        """
        setParams(self, lang="en", inputCol=None, outputCol=None):
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setLang(self, value):
        """
        Sets the value of :py:attr:`lang`.
        """
        return self._set(lang=value)

    def getLang(self):
        """
        Gets the value of lang or its default value.
        """
        return self.getOrDefault(self.lang)

    def _transform(self, dataset):
        SpacyTokenize.setup(dataset._sc, dataset.sql_ctx, self.getLang())
        func = SpacyTokenize.func(self.getLang())
        ret_type = SpacyTokenize.returnType()
        udf = pandas_udf(func, ret_type)
        return dataset.withColumn(
            self.getOutputCol(), udf(self.getInputCol())
        )


@ignore_unicode_prefix
class SpacyAdvancedTokenizeTransformer(Model, HasInputCol, HasOutputCol):
    """
    Tokenize the provided input using Spacy.
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
    >>> df = spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
    >>> tr = SpacyAdvancedTokenizeTransformer(inputCol="vals", outputCol="c")
    >>> str(tr.getLang())
    'en'
    >>> tr.getSpacyFields()
    ['_', 'ancestors', ...
    >>> tr.setSpacyFields(["text", "lang_"])
    SpacyAdvancedTokenizeTransformer_...
    >>> r = tr.transform(df).head().c
    >>> l = list(map(lambda d: sorted(d.items()), r))
    >>> l[0]
    [(u'lang_', u'en'), (u'text', u'hi')]
    >>> l[1]
    [(u'lang_', u'en'), (u'text', u'boo')]
    """

    lang = Param(Params._dummy(),
                 "lang", "language",
                 typeConverter=TypeConverters.toString)

    spacyFields = Param(Params._dummy(),
                        "spacyFields", "fields of token to keep",
                        typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, lang=None,
                 spacyFields=None,
                 inputCol=None, outputCol=None):
        super(SpacyAdvancedTokenizeTransformer, self).__init__()
        kwargs = self._input_kwargs
        if "spacyFields" not in kwargs:
            kwargs["spacyFields"] = list(SpacyAdvancedTokenize.default_fields)
        self._setDefault(lang="en",
                         spacyFields=list(SpacyAdvancedTokenize.default_fields))
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, lang="en", spacyFields=None,
                  inputCol=None, outputCol=None):
        """
        setParams(self, lang="en", SpacyAdvancedTokenize.default_fields,
                  inputCol=None, outputCol=None):
        """
        kwargs = self._input_kwargs
        if "spacyFields" not in kwargs:
            kwargs["spacyFields"] = list(SpacyAdvancedTokenize.default_fields)
        return self._set(**kwargs)

    def setLang(self, value):
        """
        Sets the value of :py:attr:`lang`.
        """
        return self._set(lang=value)

    def getLang(self):
        """
        Gets the value of lang or its default value.
        """
        return self.getOrDefault(self.lang)

    def setSpacyFields(self, value):
        """
        Sets the value of :py:attr:`spacyFields`.
        """
        return self._set(spacyFields=value)

    def getSpacyFields(self):
        """
        Gets the value of lang or its default value.
        """
        return self.getOrDefault(self.spacyFields)

    def _transform(self, dataset):
        SpacyAdvancedTokenize.setup(
            dataset._sc, dataset.sql_ctx, self.getLang())
        func = SpacyAdvancedTokenize.func(self.getLang(),
                                          self.getSpacyFields())
        ret_type = SpacyAdvancedTokenize.returnType(
            self.getLang(), self.getSpacyFields())
        udf = UserDefinedFunction(func, ret_type)
        return dataset.withColumn(
            self.getOutputCol(), udf(self.getInputCol())
        )


@ignore_unicode_prefix
class NltkPosTransformer(Model, HasInputCol, HasOutputCol):
    """
    Determine the positiveness of the sentence input.
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
    >>> df = spark.createDataFrame([("Boo is happy",), ("sad Boo",),
    ...                             ("i enjoy rope burn",)], ["vals"])
    >>> tr = NltkPosTransformer(inputCol="vals", outputCol="c")
    >>> tr.transform(df).show()
    +-----------------+-----+
    |             vals|    c|
    +-----------------+-----+
    |     Boo is happy|0.6...|
    |          sad Boo|  0.0|
    |i enjoy rope burn|0.6...|
    +-----------------+-----+...
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(NltkPosTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol=None, outputCol=None):
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        func = NltkPos.func()
        ret_type = NltkPos.returnType()
        udf = pandas_udf(func, ret_type, PandasUDFType.SCALAR)
        return dataset.withColumn(
            self.getOutputCol(), udf(self.getInputCol())
        )


if __name__ == '__main__':
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
