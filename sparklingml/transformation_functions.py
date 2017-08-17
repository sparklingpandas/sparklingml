from pyspark.sql.types import *

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
class SpacyMagic(object):
    """
    Simple Spacy Magic to minimize loading time.
    >>> SpacyMagic.get("en")
    <spacy.en.English ...
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
    ['hi', 'boo']
    """
    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        import spacy
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


class SpacyAdvancedTokenize(TransformationFunction):
    """
    Tokenize input text using spacy and return the extra information.
    >>> spta = SpacyAdvancedTokenize()
    >>> spa = spta.func("en", ["lower_", "text", "lang"])
    >>> r = spa("Hi boo")
    >>> l = list(map(lambda d: sorted(d.items()), r))
    >>> l[0]
    [('lang', '500'), ('lower_', 'hi'), ('text', 'Hi')]
    >>> l[1]
    [('lang', '500'), ('lower_', 'boo'), ('text', 'boo')]
    """

    default_fields = [
        'ancestors', 'check_flag', 'children', 'cluster', 'conjuncts', 'dep',
        'ent_id', 'ent_iob', 'ent_type', 'has_repvec', 'has_vector', 'head',
        'i', 'idx', 'is_alpha', 'is_ancestor', 'is_ancestor_of', 'is_ascii',
        'is_bracket', 'is_digit', 'is_left_punct', 'is_lower', 'is_oov',
        'is_punct', 'is_quote', 'is_right_punct', 'is_space', 'is_stop',
        'is_title', 'lang', 'lang_', 'left_edge', 'lefts', 'lemma',
        'lemma_', 'lex_id', 'like_email', 'like_num', 'like_url',
        'lower', 'lower_', 'n_lefts', 'n_rights', 'nbor', 'norm',
        'norm_', 'orth', 'orth_', 'pos', 'pos_', 'prefix', 'prefix_',
        'prob', 'rank', 'repvec', 'right_edge', 'rights', 'sentiment', 'shape',
        'shape_', 'similarity', 'string', 'subtree', 'suffix', 'suffix_',
        'tag', 'tag_', 'text', 'text_with_ws', 'vector', 'vector_norm',
        'vocab', 'whitespace_']

    @classmethod
    def setup(cls, sc, session, *args):
        pass

    @classmethod
    def func(cls, *args):
        import spacy
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
                    except:
                        return (field_name, None)
                return dict(map(lookup_field_or_none, fields))
            return list(map(spacyTokenToDict, list(nlp(inputString))))
        return inner

    @classmethod
    def returnType(cls, *args):
        return ArrayType(MapType(StringType(), StringType()))

functions_info["spacyadvancedtokenize"] = SpacyAdvancedTokenize

if __name__ == '__main__':
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS)
