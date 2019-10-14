#!/bin/bash
set -x
set -e


echo "Installing python requirements"

pip install -r requirements.txt

echo "Checking if spacy 'en' is installed otherwise download it"

python -c "import spacy;spacy.load('en_core_web_sm')" || python -m spacy download en_core_web_sm && python -c "import spacy;spacy.load('en_core_web_sm')"

echo "Checking scala style issues"

./build/sbt scalastyle

echo "Checking python style issues"

flake8 --ignore=E402,F405,F401,F403 sparklingml/

echo "Building JVM code"

./build/sbt clean pack assembly

echo "Copying assembly jar to python loadable directory"

mkdir -p ./sparklingml/jar
cp target/scala-2.11/sparklingml-assembly-0.0.1-SNAPSHOT.jar ./sparklingml/jar/sparklingml.jar

echo "Testing Python code"

nosetests --logging-level=INFO --detailed-errors --verbosity=2 --with-coverage --cover-html-dir=./htmlcov --cover-package=sparklingml --with-doctest --doctest-options=+ELLIPSIS,+NORMALIZE_WHITESPACE

echo "Testing pip install of Python code"

pip install .
mkdir /tmp/abcd
pushd /tmp/abcd
python -c "import sparklingml"
popd


echo "Testing JVM code"

# Skip for now due to gateway issues.
# ./build/sbt test

echo "Finished"
