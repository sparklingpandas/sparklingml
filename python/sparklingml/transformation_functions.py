from pyspark.sql.types import IntegerType

functions_info = dict()

class TransformationFunction(object):
    @classmethod
    def returnType(cls):
        """Return the sql return type"""
        return None

    @classmethod
    def func(cls, args):
        """Returns a function constructed using the args."""
        return None
    
    

class StrLenPlusK(TransformationFunction):
    """
    strLenPlusK takes one parameter it is k and returns
    the string length plus k. This is intended to illustrate how
    to make a Python stage usable from Scala, not for actual usage.
    """
    @classmethod
    def func(cls, k):
        def inner(str):
            """Compute the string length plus K (based on parameters)."""
            return len(str) + k
        return inner

    @classmethod
    def returnType(cls):
        return IntegerType()

functions_info["strlenplusk"] = StrLenPlusK
