# The shared params aren't really intended to be public currently..
from pyspark import keyword_only
from pyspark.ml import Model
from pyspark.ml.param import *
from pyspark.ml.param.shared import *
from pyspark.ml.util import *
from pyspark.ml.common import inherit_doc
from pyspark.sql import DataFrame
from pyspark.sql.functions import UserDefinedFunction
from transformation_functions import *

# Most of the Python models are wrappers of JavaModels, and we will need those
# as well. For now this simple example shows how to expose a simple Python only
# UDF. Look in Spark for exposing Java examples.


class StrLenPlusKTransformer(Model, HasInputCol, HasOutputCol):
    """
    strLenPlusK takes one parameter it is k and returns
    the string length plus k. This is intended to illustrate how
    to make a Python stage and not for actual use.
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
    >>> df = spark.createDataFrame([("hi",), ("boo",)], ["values"])
    >>> tr = StrLenPlusKTransformer(inputCol="values", outputCol="c", k=2)
    >>> tr.transform(df).head().c
    4
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
        retType = StrLenPlusK.returnType()
        udf = UserDefinedFunction(func, retType)
        return dataset.withColumn(
            self.getOutputCol(), udf(self.getInputCol())
        )


if __name__ == '__main__':
    import doctest
    doctest.testmod()
