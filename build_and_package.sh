#!/bin/bash
set -x
set -e

echo "Installing python requirements"

pip install -r requirements.txt

echo "Checking if spacy 'en' is installed otherwise download it"

python -c "import spacy;spacy.load('en')" || python -m spacy download en

echo "Checking scala style issues"

./build/sbt scalastyle

echo "Checking python style issues"

pep8 --ignore=E402 sparklingml/

echo "Building JVM code"

./build/sbt clean pack

echo "Testing Python code"

nosetests --logging-level=INFO --detailed-errors --verbosity=2 --with-coverage --cover-html-dir=./htmlcov --cover-package=sparklingml --with-doctest --doctest-options=+ELLIPSIS,+NORMALIZE_WHITESPACE


echo "Testing JVM code"

./build/sbt test

echo "Finished"
