#!/bin/bash
set -x
set -e

echo "Checking scala style issues"

./build/sbt scalastyle

echo "Checking python style issues"

pep8 --ignore=E402 sparklingml/

echo "Building and testing JVM code"

./build/sbt clean compile package test

echo "Testing Python code"

nosetests --logging-level=INFO --detailed-errors --verbosity=2 --with-coverage --cover-html-dir=./htmlcov --cover-package=sparklingml --with-doctest --doctest-options=+ELLIPSIS
