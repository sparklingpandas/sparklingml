![sparkling ml logo](https://raw.githubusercontent.com/sparklingpandas/sparklingml/master/imgs/sparkling_ml.png)
[![buildstatus](https://travis-ci.org/sparklingpandas/sparklingml.svg?branch=master)](https://travis-ci.org/sparklingpandas/sparklingml)
[![codecov.io](http://codecov.io/github/sparklingpandas/sparklingml/coverage.svg?branch=master)](http://codecov.io/github/sparklingpandas/sparklingml?branch=master)

# sparklingml
Machine Learning Pipeline Stages for Spark (exposed in Scala/Java + Python)

## Why?

SparklingML's goal is to expose additional machine learning stages for Spark with the pipeline interface.

## Status

Super early! Come join!

Dev mailing list: https://groups.google.com/forum/#!forum/sparklingml-dev

## Building

Sparkling ML consists of two components, a Python component and a Java/Scala component. The Python component depends on having the Java/Scala component pre-build which can be done by running `./build/sbt package`.


The Python component depends on the package listed in requirements.txt (as well as part of setup.py). Development and testing also requires spacy, nose, codecov, pylint, and flake8.


The script `build_and_package.sh` builds & tests both the Scala and Python code.


For now this only works with Spark 2.3.2, it needs some changes to support other versions.

### Tests

Are your DocTests failing with

>Expected nothing
>Got:
>    <BLANKLINE>
>        Warning: no model found for 'en'
>    <BLANKLINE>
>        Only loading the 'en' tokenizer.
>    <BLANKLINE>


Make sure you've installed spacy & the en language pack (`python -m spacy download en`)

## Including in your build

SparklingML is not yet ready for production use.

## License

SparklingML is licensed under the Apache 2 license. Some additional components may be under a different license.
