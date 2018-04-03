#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Sparkling ML provides additional ML algorithms for Spark.
"""
from __future__ import print_function
import glob
import os
from pkg_resources import resource_filename


if 'IS_TEST' not in os.environ and "JARS" not in os.environ:
    VERSION = '0.0.1'
    # Check if target pack exists
    jar = None
    if os.path.exists("target/pack/lib/"):
        print("Using packed jars during development")
        jars = glob.glob("target/pack/lib/*.jar")
        abs_path_jars = map(os.path.abspath, jars)
        jar = ",".join(abs_path_jars)
    else:
        JAR_FILE = 'sparklingml-assembly-' + VERSION + '.jar'
        DEV_JAR = 'sparklingml-assembly-' + VERSION + '-SNAPSHOT.jar'
        my_location = os.path.dirname(os.path.realpath(__file__))
        local_prefixes = [
            # For development, use the sbt target scala-2.11 first
            # since the init script is in sparklingpandas move up one dir
            os.path.join(my_location, '../target/scala-2.11/'),
            # Also try the present working directory
            os.path.join(os.getcwd(), '../target/scala-2.11/'),
            os.path.join(os.getcwd(), 'target/scala-2.11/')]
        prod_jars = [os.path.join(prefix, JAR_FILE)
                     for prefix in local_prefixes]
        dev_jars = [os.path.join(prefix, DEV_JAR)
                    for prefix in local_prefixes]
        jars = prod_jars + dev_jars
        try:
            jars.append(os.path.abspath(resource_filename('sparklingml.jar',
                                                          JAR_FILE)))
        except Exception as e:
            print("Could not resolve resource file %s. This is not necessarily"
                  " (and is expected during development) but should not occur "
                  "in production if pip installed." % str(e))
        try:
            jar = [jar_path
                   for jar_path in jars
                   if os.path.exists(jar_path)][0]
        except IndexError:
            print("Failed to find jars. Looked at paths %s." % jars)
            if 'SPARKLING_ML_SPECIFIC' not in os.environ:
                raise IOError("Failed to find jars. Looked at paths %s."
                              % jars)
            else:
                print("Failed to find jars, but launched from the JVM"
                      "so this _should_ be ok.")
    if jar is not None:
        os.environ["JARS"] = jar
        print("Using backing jar " + jar)
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--jars %s --driver-class-path %s pyspark-shell") % (jar, jar)
