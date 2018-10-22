from __future__ import unicode_literals

import inspect
import sys
import threading

import pandas  # noqa: F401
import spacy
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import *

if sys.version_info.major == 3:
    unicode = str

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
    >>> spm = SpacyMagic()
    >>> spm2 = SpacyMagic()
    >>> spm == spm2
    True
    >>> spm.get("en")
    <spacy.lang.en.English ...
    >>> spm.get("non-happy-language")
    Traceback (most recent call last):
      ...
    Exception: Failed to find or download language non-happy-language:...
    >>> spm.broadcast()
    <pyspark.broadcast.Broadcast ...
    """
    _spacys = {}
    _broadcast = None
    _self = None
    __empty_please = False
    __lock = threading.Lock()

    def __new__(cls):
        """Not quite a proper singleton but I'm lazy."""
        if not cls._self:
            cls._self = super(SpacyMagic, cls).__new__(cls)
        return SpacyMagic._self

    def get(self, lang):
        if lang not in self._spacys:
            import spacy
            # Hack to dynamically download languages on cluster machines, you can
            # remove if you have the models installed and just do:
            # cls._spacys[lang] = spacy.load(lang)
            try:
                old_exit = sys.exit
                sys.exit = None
                try:
                    self._spacys[lang] = spacy.load(lang)
                except Exception:
                    spacy.cli.download(lang)
                    self._spacys[lang] = spacy.load(lang)
            except Exception as e:
                raise Exception(
                    "Failed to find or download language {0}: {1}"
                    .format(lang, e))
            finally:
                sys.exit = old_exit

        return self._spacys[lang]

    def broadcast(self):
        """Broadcast self to ensure we are shared."""
        if self._broadcast is None:
            from pyspark.context import SparkContext
            sc = SparkContext.getOrCreate()
            try:
                SpacyMagic.__lock.acquire()
                self.__empty_please = True
                self._broadcast = sc.broadcast(self)
                self.__empty_please = False
            finally:
                SpacyMagic.__lock.release()
        return self._broadcast

    def __getstate__(self):
        """Get state, don't serialize anything."""
        if not self.__empty_please:
            return self.broadcast()
        else:
            return None

    def __setstate__(self, bcast):
        """Set state, from the broadcast."""
        self = bcast.value
        


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
        spm = SpacyMagic()

        def inner(inputSeries):
            """Tokenize the inputString using spacy for
            the provided language."""
            nlp = spm.get(lang)

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
        spm = SpacyMagic()

        def inner(inputString):
            """Tokenize the inputString using spacy for the provided language.
            Keeps the requested fields."""
            nlp = spm.get(lang)

            def spacyTokenToDict(token):
                """Convert the input token into a dictionary"""
                def lookup_field_or_none(field_name):
                    try:
                        return (field_name, str(getattr(token, field_name)))
                    except AttributeError:
                        return (field_name, None)
                return dict(map(lookup_field_or_none, fields))
            return list(map(spacyTokenToDict, list(nlp(unicode(inputString)))))
        return inner

    @classmethod
    def returnType(cls, *args):
        return ArrayType(MapType(StringType(), StringType()))


functions_info["spacyadvancedtokenize"] = SpacyAdvancedTokenize


@ignore_unicode_prefix
class NltkPos(ScalarVectorizedTransformationFunction):
    """
    Uses nltk to compute the positive sentiment of the input expression.
    >>> sentences = pandas.Series(["Boo is happy", "Boo is sad", "confused."])
    >>> myFunc = NltkPos().func()
    >>> import math
    >>> myFunc(sentences).apply(math.ceil)
    0    1...
    1    0...
    2    0...
    dtype: ...
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
