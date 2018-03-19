import ast

from py4j.java_gateway import *
# Spark imports
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import UserDefinedFunction, PandasUDFType

from sparklingml.transformation_functions import *

# Hack to allow people to hook in more easily
try:
    from user_functions import *
    setup_user()
except ImportError:
    pass


# This class is used to allow the Scala process to call into Python
# It may not run in the same Python process as your regular Python
# shell if you are running PySpark normally.
class PythonRegistrationProvider(object):
    """
    Provide an entry point for Scala to call to register functions.
    """

    def __init__(self, gateway):
        self.gateway = gateway
        self._sc = None
        self._session = None
        self._count = 0

    def registerFunction(self, ssc, jsession, function_name, params):
        jvm = self.gateway.jvm
        if not self._sc:
            master = ssc.master()
            jsc = jvm.org.apache.spark.api.java.JavaSparkContext(ssc)
            jsparkConf = ssc.conf()
            sparkConf = SparkConf(_jconf=jsparkConf)
            self._sc = SparkContext(
                master=master,
                conf=sparkConf,
                gateway=self.gateway,
                jsc=jsc)
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
        if function_name in functions_info:
            function_info = functions_info[function_name]
            if params:
                evaledParams = ast.literal_eval(params)
            else:
                evaledParams = []
            func = function_info.func(*evaledParams)
            ret_type = function_info.returnType()
            self._count = self._count + 1
            registration_name = function_name + str(self._count)
            if isinstance(function_info, ScalarVectorizedTransformationFunction):
                udf = pandas_udf(func, ret_type, PandasUDFType.SCALAR)
            else:
                udf = UserDefinedFunction(func, ret_type, registration_name)
            return udf._judf
        else:
            print("Could not find function")
            return None

    class Java:
        package = "com.sparklingpandas.sparklingml.util.python"
        className = "PythonRegisterationProvider"
        implements = [package + "." + className]

if __name__ == "__main__":
    def spark_jvm_imports(jvm):
        # Import the classes used by PySpark
        java_import(jvm, "org.apache.spark.SparkConf")
        java_import(jvm, "org.apache.spark.api.java.*")
        java_import(jvm, "org.apache.spark.api.python.*")
        java_import(jvm, "org.apache.spark.ml.python.*")
        java_import(jvm, "org.apache.spark.mllib.api.python.*")
        # TODO(davies): move into sql
        java_import(jvm, "org.apache.spark.sql.*")
        java_import(jvm, "org.apache.spark.sql.hive.*")
        java_import(jvm, "scala.Tuple2")

    import os
    if "SPARKLING_ML_SPECIFIC" in os.environ:
        # Py4J setup work so we can talk
        gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
        gateway = JavaGateway(
            GatewayClient(port=gateway_port),
            # TODO: handle dynamic port binding here correctly.
            callback_server_parameters=CallbackServerParameters(port=0),
            auto_convert=True)
        # retrieve the port on which the python callback server was bound to.
        python_port = gateway.get_callback_server().get_listening_port()
        # bind the callback server on the java side to the new python_port
        gateway.java_gateway_server.resetCallbackClient(
            gateway.java_gateway_server.getCallbackClient().getAddress(),
            python_port)
        # Create our registration provider interface for Py4J to call into
        provider = PythonRegistrationProvider(gateway)
        # Sparkling pandas specific imports
        jvm = gateway.jvm
        java_import(jvm, "com.sparklingpandas.sparklingml")
        java_import(jvm, "com.sparklingpandas.sparklingml.util.python")
        # We need to re-do the Spark gateway imports as well
        spark_jvm_imports(jvm)
        python_utils = jvm.com.sparklingpandas.sparklingml.util.python
        pythonRegistrationObj = python_utils.PythonRegistration
        boople = jvm.org.apache.spark.SparkConf(False)
        pythonRegistrationObj.register(provider)
        # Busy loop so we don't exit. This is also kind of a hack.
        import time
        while (True):
            time.sleep(1)
        print("real exit")
