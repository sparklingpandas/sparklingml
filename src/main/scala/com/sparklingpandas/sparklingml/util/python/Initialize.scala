/**
 * Initialize SparklingML on a given environment. This is done to allow
 * SparklingML to setup Python callbacks and does not need to be called
 * in the Python side. This is a little ugly, improvements are especially
 * welcome.
 */
package com.sparklingpandas.sparklingml.util.python

import java.io._

import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer
import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.util.Success

import org.apache.spark.SparkContext
import org.apache.spark.deploy.PythonRunner._
import org.apache.spark.sql._
import org.apache.spark.internal.config._
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction

import py4j.GatewayServer

/**
 * Abstract trait to implement in Python to allow Scala to call in to perform
 * registration.
 */
trait PythonRegisterationProvider {
  // Takes a SparkContext, SparkSession, String, and String
  // Returns UserDefinedPythonFunction but types + py4j :(
  def registerFunction(
    sc: SparkContext, session: Object,
    functionName: Object, params: Object): Object
}

/**
 * A utility class to redirect the child process's stdout or stderr.
 * This is copied from Spark.
 */
private[python] class RedirectThread(
    in: InputStream,
    out: OutputStream,
    name: String,
    propagateEof: Boolean = false)
  extends Thread(name) {

  setDaemon(true)
  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      tryWithSafeFinally {
        val buf = new Array[Byte](1024)
        Iterator.continually(in.read(buf))
          .takeWhile(_ != -1)
          .foreach{len =>
          out.write(buf, 0, len)
          out.flush()
        }
      } {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
  /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   *
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    val ret = try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        try {
          finallyBlock
        } catch {
          case t2: Throwable =>
            t.addSuppressed(t2)
        }
        throw t
    }
    finallyBlock
    ret
  }
}


object PythonRegistration {
  val pythonFile = "./python/sparklingml/startup.py"
  val pyFiles = ""
  // TODO(holden): Use reflection to determine if we've got an existing gateway server
  // to hijack instead.
  val gatewayServer: GatewayServer = {
    // Based on PythonUtils
    def sparkPythonPath: String = {
      val pythonPath = new ArrayBuffer[String]
      for (sparkHome <- sys.env.get("SPARK_HOME")) {
        pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
        pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.10.6-src.zip").mkString(File.separator)
      }
      pythonPath ++= SparkContext.jarOfObject(this)
      pythonPath.mkString(File.pathSeparator)
    }
    def mergePythonPaths(paths: String*): String = {
      paths.filter(_ != "").mkString(File.pathSeparator)
    }

    // Format python file paths before adding them to the PYTHONPATH
    val formattedPythonFile = formatPath(pythonFile)
    val formattedPyFiles = formatPaths(pyFiles)

    // Launch a gatewayserver to handle registration, based on PythonRunner.scala
    val sparkConf = SparkContext.getOrCreate().getConf
    // Format python file paths before adding them to the PYTHONPATH
    val pythonExec = sparkConf.getOption("spark.pyspark.driver.python")
      .orElse(sparkConf.getOption("spark.pyspark.python"))
      .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("python")
    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val gatewayServer = new py4j.GatewayServer(PythonRegistration, 0)
    val thread = new Thread(new Runnable() {
      override def run(): Unit = {
        gatewayServer.start(true)
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port is it bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()

    // Build up a PYTHONPATH that includes the Spark assembly (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = mergePythonPaths(pathElements: _*)

    // Launch Python process
    val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile)).asJava)
    val env = builder.environment()
    env.put("SPARKLING_ML_SPECIFIC", "YES")
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    // pass conf spark.pyspark.python to python process, the only way to pass info to
    // python process is through environment variable.
    env.put("PYSPARK_PYTHON", pythonExec)
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val pythonThread = new Thread(new Runnable() {
      override def run(): Unit = {
        try {
          val process = builder.start()

          new RedirectThread(process.getInputStream, System.out, "redirect output").start()

          val exitCode = process.waitFor()
          if (exitCode != 0) {
            throw new Exception(s"Exit code ${exitCode}")
          }
        } finally {
          gatewayServer.shutdown()
        }
      }
    })
    pythonThread.setName("python-udf-registrationProvider-thread")
    pythonThread.setDaemon(true)
    pythonThread.start()
    gatewayServer
  }

  def register(provider: PythonRegisterationProvider) = {
    pythonRegistrationProvider.complete(Success(provider))
  }

  val pythonRegistrationProvider = Promise[PythonRegisterationProvider]()
}
