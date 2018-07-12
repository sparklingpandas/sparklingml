from __future__ import unicode_literals

import inspect

import pandas
import spacy
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType

functions_info = dict()


class TransformationFunction(object):
    @classmethod
    def setup(cls, sc, session, *args):
        """Perform any setup work (like global broadcasts)"""
        pass

    @classmethod
    def returnType(cls, *args):
        """Return the sql return type"""
        return None

    @classmethod
    def func(cls, *args):
        """Returns a function constructed using the args."""
        return None

    @classmethod
    def evalType(cls):
        """Returns the eval type to be used."""
        from pyspark.rdd import PythonEvalType
        return PythonEvalType.SQL_BATCHED_UDF


class ScalarVectorizedTransformationFunction(TransformationFunction):
    """Transformation functions which are Scalar Vectorized UDFS."""

    @classmethod
    def evalType(cls):
        """Returns the eval type to be used."""
        return PandasUDFType.SCALAR


@ignore_unicode_prefix
class StrLenPlusK(TransformationFunction):
    """
    strLenPlusK takes one parameter it is k and returns
    the string length plus k. This is intended to illustrate how
    to make a Python stage usable from Scala, not for actual usage.
    """
    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        k = args[0]

        def inner(inputString):
            """Compute the string length plus K (based on parameters)."""
            return len(inputString) + k
        return inner

    @classmethod
    def returnType(cls, *args):
        return IntegerType()


functions_info["strlenplusk"] = StrLenPlusK


# Spacy isn't serializable but loading it is semi-expensive
@ignore_unicode_prefix
class SpacyMagic(object):
    """
    Simple Spacy Magic to minimize loading time.
    >>> SpacyMagic.get("en")
    <spacy.lang.en.English ...
    """
    _spacys = {}

    @classmethod
    def get(cls, lang):
        if lang not in cls._spacys:
            import spacy
            cls._spacys[lang] = spacy.load(lang)
        return cls._spacys[lang]


class SpacyTokenize(ScalarVectorizedTransformationFunction):
    """
    Tokenize input text using spacy.
    >>> spt = SpacyTokenize()
    >>> sp = spt.func("en")
    >>> r = sp(pandas.Series(["hi boo"]))
    ...
    >>> r
    0    [hi, boo]
    dtype: object
    """
    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        lang = args[0]

        def inner(inputSeries):
            """Tokenize the inputString using spacy for
            the provided language."""
            nlp = SpacyMagic.get(lang)

            def tokenizeElem(elem):
                result_itr = map(lambda token: token.text,
                                 list(nlp(unicode(elem))))
                return list(result_itr)

            return inputSeries.apply(tokenizeElem)
        return inner

    @classmethod
    def returnType(cls, *args):
        return ArrayType(StringType())


functions_info["spacytokenize"] = SpacyTokenize


@ignore_unicode_prefix
class SpacyAdvancedTokenize(TransformationFunction):
    """
    Tokenize input text using spacy and return the extra information.
    >>> spta = SpacyAdvancedTokenize()
    >>> spa = spta.func("en", ["lower_", "text", "lang", "a"])
    >>> r = spa("Hi boo")
    >>> l = list(map(lambda d: sorted(d.items()), r))
    >>> l[0]
    [(u'a', None), (u'lang', '...'), (u'lower_', 'hi'), (u'text', 'Hi')]
    >>> l[1]
    [(u'a', None), (u'lang', '...'), (u'lower_', 'boo'), (u'text', 'boo')]
    """

    default_fields = map(
        lambda x: x[0],
        inspect.getmembers(spacy.tokens.Token,
                           lambda x: "<attribute '" in repr(x)))

    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        lang = args[0]
        fields = args[1]

        def inner(inputString):
            """Tokenize the inputString using spacy for the provided language.
            Keeps the requested fields."""
            nlp = SpacyMagic.get(lang)

            def spacyTokenToDict(token):
                """Convert the input token into a dictionary"""
                def lookup_field_or_none(field_name):
                    try:
                        return (field_name, str(getattr(token, field_name)))
                    except AttributeError:
                        return (field_name, None)
                return dict(map(lookup_field_or_none, fields))
            return list(map(spacyTokenToDict, list(nlp(inputString))))
        return inner

    @classmethod
    def returnType(cls, *args):
        return ArrayType(MapType(StringType(), StringType()))


functions_info["spacyadvancedtokenize"] = SpacyAdvancedTokenize


@ignore_unicode_prefix
class NltkPos(ScalarVectorizedTransformationFunction):
    """
    Uses nltk to compute the positive sentiment of the input expression.
    >>> sentences = pandas.Series(["Boo is happy", "Boo is sad"])
    >>> myFunc = NltkPos().func()
    >>> import math
    >>> myFunc(sentences).apply(math.ceil)
    0    1.0...
    1    0.0...
    dtype: float64
    """

    @classmethod
    def func(cls, *args):
        def inner(input_series):
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            # Hack until https://github.com/nteract/coffee_boat/issues/47
            try:
                sid = SentimentIntensityAnalyzer()
            except LookupError:
                import nltk
                nltk.download('vader_lexicon')
                sid = SentimentIntensityAnalyzer()
            result = input_series.apply(
                lambda sentence: sid.polarity_scores(sentence)['pos'])
            return result
        return inner

    @classmethod
    def returnType(cls, *args):
        return DoubleType()


functions_info["nltkpos"] = NltkPos

if __name__ == '__main__':
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
