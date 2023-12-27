package oracle.datahub.spark.prod

import com.google.common.collect.Lists
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Files
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results

class DataHubIMain {
  var _datahubILoop: DataHubILoop = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  def startRepl(): DataHubILoop = {
    val tmpdir: String = Files.createTempDirectory("repl").toFile.getAbsolutePath
    val tmpDirsLocation: String = System.getProperty("java.io.tmpdir")
    val outputDir = tmpdir
    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir}"), true)
    settings.usejavacp.value = true
    //val in = new BufferedReader(new StringReader(input + "\n"))
    _datahubILoop = new DataHubILoop(null, new PrintWriter(new StringWriter()))
    _datahubILoop.run(settings)
    Thread.currentThread.setContextClassLoader(_datahubILoop.classLoader)
    createSparkContextAndBind()
    _datahubILoop
  }

  def createSparkContextAndBind(): SparkContext = {
    sc = new SparkContext(createSparkConf())
    sqlContext = new SQLContext(sc)
    bind("sc", "org.apache.spark.SparkContext", sc, Lists.newArrayList("@transient"))
    bind("sqlContext", "org.apache.spark.sql.SQLContext", sqlContext, Lists.newArrayList("@transient"))

    _datahubILoop.interpret("import org.apache.spark.SparkContext._")
    _datahubILoop.interpret("import spark.implicits._")
    _datahubILoop.interpret("import sqlContext.implicits._")
    _datahubILoop.interpret("import spark.sql")
    _datahubILoop.interpret("import org.apache.spark.sql.functions._")
    // print empty string otherwise the last statement's output of this method// print empty string otherwise the last statement's output of this method
    // (aka. import org.apache.spark.sql.functions._) will mix with the output of user code// (aka. import org.apache.spark.sql.functions._) will mix with the output of user code
    _datahubILoop.interpret("print(\"\")")
    sc
  }

  def createSparkConf(): SparkConf = {
    val master = "local[2]"
    val sparkConf = new SparkConf().setMaster(master).setAppName("DataHub Shell")
    sparkConf
  }

  def getScalaShellClassLoader: ClassLoader = {
    _datahubILoop.classLoader
  }

  def close(): Unit = {
    if (_datahubILoop != null) {
      _datahubILoop.closeInterpreter()
    }
  }

  private def bind(name: String, tpe: String, value: Object, modifier: List[String]): Unit = {
    _datahubILoop.beQuietDuring {
      val result = _datahubILoop.bind(name, tpe, value, modifier)
      if (result != Results.Success) {
        throw new RuntimeException("Fail to bind variable: " + name)
      }
    }
  }

  def bind(name: String,
           tpe: String,
           value: Object,
           modifier: java.util.List[String]): Unit =
    bind(name, tpe, value, modifier.asScala.toList)

}
