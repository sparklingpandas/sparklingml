from __future__ import unicode_literals

from pyspark.ml.wrapper import JavaModel, JavaTransformer


class SparklingJavaTransformer(JavaTransformer):
    """
    Base class for Java transformers exposed in Python.
    """
    def __init__(self, jt=None):
        super(SparklingJavaTransformer, self).__init__()
        if not jt:
            self._java_obj = self._new_java_obj(self.transformer_name)
        else:
            self._java_obj = jt


class SparklingJavaModel(JavaModel):
    """
    Base class for Java mdels exposed in Python.
    """
    def __init__(self, jm=None):
        super(SparklingJavaModel, self).__init__()
        if not jm:
            self._java_obj = self._new_java_obj(self.model_name)
        else:
            self._java_obj = jm
