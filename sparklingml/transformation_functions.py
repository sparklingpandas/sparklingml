from __future__ import unicode_literals

import inspect

import spacy
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf

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


class SpacyTokenize(TransformationFunction):
    """
    Tokenize input text using spacy.
    >>> spt = SpacyTokenize()
    >>> sp = spt.func("en")
    >>> r = sp("hi boo")
    ...
    >>> r
    [u'hi', u'boo']
    """
    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        lang = args[0]

        def inner(inputString):
            """Tokenize the inputString using spacy for
            the provided language."""
            nlp = SpacyMagic.get(lang)
            return list(map(lambda x: x.text, list(nlp(inputString))))
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
class NltkPos(TransformationFunction):
    """
    Uses nltk to compute the positive sentiment of the input expression.
    """

    @classmethod
    def func(cls, *args):
        @pandas_udf(returnType=DoubleType())
        def inner(input_series):
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            # Hack until https://github.com/nteract/coffee_boat/issues/47 is fixed
            try:
                sid = SentimentIntensityAnalyzer()
            except LookupError:
                import nltk
                nltk.download('vader_lexicon')
                sid = SentimentIntensityAnalyzer()
            result = input_series.apply(lambda sentence: sid.polarity_scores(sentence)['pos'])
            return result
        return inner

    @classmethod
    def returnType(cls, *args):
        return DoubleType()



if __name__ == '__main__':
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
