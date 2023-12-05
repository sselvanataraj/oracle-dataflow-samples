package  oracle.datahub.spark.backend.interpreter


import oracle.datahub.spark.backend.interpreter.util.InterpreterOutputStream
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, File, InputStreamReader, PipedInputStream, PrintWriter, StringWriter}
import java.net.URLClassLoader
import java.nio.file.Paths
import scala.tools.nsc.interpreter.Results
import scala.tools.nsc.interpreter.shell.{Accumulator, Completion, ReplCompletion}
import scala.jdk.CollectionConverters._
import scala.tools.nsc.Settings

/**
 * Inner Scala interpreter.
 * @param conf Spark Configurations
 * @param interpreterClassLoader User class loader
 * @param interpreterOutputDir interpreter output
 */
class ScalaInnerInterpreter(conf: SparkConf,
                      interpreterClassLoader: URLClassLoader,
                      interpreterOutputDir: File){
  private lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val MASTER_PROP_NAME = "spark.master"
  val DEFAULT_MASTER_VALUE = "local[*]"

  private var sparkILoop: SparkILoop = _
  private var scalaCompletion: Completion = _
  private val sparkMaster: String = conf.get(MASTER_PROP_NAME, DEFAULT_MASTER_VALUE)
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)

  private def bind(name: String, tpe: String, value: Object, modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      val result = sparkILoop.bind(name, tpe, value, modifier)
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

  def getScalaShellClassLoader: ClassLoader = {
    sparkILoop.classLoader
  }

  def scalaInterpret(code: String): scala.tools.nsc.interpreter.Results.Result =
    sparkILoop.interpret(code)

  def scalaInterpretQuietly(code: String): Unit = {
    scalaInterpret(code) match {
      case scala.tools.nsc.interpreter.Results.Success =>
      // do nothing
      case scala.tools.nsc.interpreter.Results.Error =>
        throw new RuntimeException("Fail to run code: " + code)
      case scala.tools.nsc.interpreter.Results.Incomplete =>
        throw new RuntimeException("Incomplete code: " + code)
    }
  }

  /**
   * Get user jars and loads using {@link MutableURLClassLoader} for current Thread.
   * @return
   */
  private def getUserJars(): Seq[String] = {
    var classLoader = Thread.currentThread().getContextClassLoader
    var extraJars = Seq.empty[String]
    while (classLoader != null) {
      if (classLoader.getClass.getCanonicalName ==
        "org.apache.spark.util.MutableURLClassLoader") {
        extraJars = classLoader.asInstanceOf[URLClassLoader].getURLs()
          // Check if the file exists.
          .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
          // Some bad spark packages depend on the wrong version of scala-reflect. Blacklist it.
          .filterNot {
            u => Paths.get(u.toURI).getFileName.toString.contains("org.scala-lang_scala-reflect")
          }
          .map(url => url.toString).toSeq
        classLoader = null
      } else {
        classLoader = classLoader.getParent
      }
    }

    extraJars ++= interpreterClassLoader.getURLs().map(_.getPath())
    println("User jar for spark repl: " + extraJars.mkString(","))
    extraJars
  }

  /**
   * Creates custom Spark Scala REPL
   */
  def createSparkILoop(): Unit = {
    // To support multiple repls
    println("Scala shell repl output dir: " + interpreterOutputDir.getAbsolutePath)
    conf.set("spark.repl.class.outputDir", interpreterOutputDir.getAbsolutePath)
    conf.setAppName("DataHub Shell")

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${interpreterOutputDir.getAbsolutePath}"), true)
    settings.embeddedDefaults(interpreterClassLoader)
    settings.usejavacp.value = true

    // Load user jars
    val userJars = getUserJars()
    println("UserJars: " + userJars.mkString(File.pathSeparator))
   // settings.classpath.value = userJars.mkString(File.pathSeparator)
    settings.classpath.value = System.getProperty("java.class.path");

    // Create ScalaILoop
    val replOut = new PrintWriter(Console.out, true)
    sparkILoop = new SparkILoop(
      new BufferedReader(new InputStreamReader(new PipedInputStream())),
      replOut)
    sparkILoop.run(settings)
    this.scalaCompletion = new ReplCompletion(sparkILoop.intp, new Accumulator)
    Thread.currentThread.setContextClassLoader(sparkILoop.classLoader)
  }

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    val originalOut = System.out
    val printREPLOutput = context.getStringLocalProperty("printREPLOutput", "true").toBoolean

    def _interpret(code: String): scala.tools.nsc.interpreter.Results.Result = {
      Console.withOut(interpreterOutput) {
        System.setOut(Console.out)
        if (printREPLOutput) {
          interpreterOutput.setInterpreterOutput(context.out)
        } else {
          interpreterOutput.setInterpreterOutput(null)
        }
        interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()

        val status = scalaInterpret(code) match {
          case success@scala.tools.nsc.interpreter.Results.Success =>
            success
          case scala.tools.nsc.interpreter.Results.Error =>
            val errorMsg = new String(interpreterOutput.getInterpreterOutput.toByteArray)
            if (errorMsg.contains("value toDF is not a member of org.apache.spark.rdd.RDD") ||
              errorMsg.contains("value toDS is not a member of org.apache.spark.rdd.RDD")) {
              // prepend "import sqlContext.implicits._" due to
              // https://issues.scala-lang.org/browse/SI-6649
              context.out.clear()
              scalaInterpret("import sqlContext.implicits._\n" + code)
            } else {
              scala.tools.nsc.interpreter.Results.Error
            }
          case scala.tools.nsc.interpreter.Results.Incomplete =>
            // add print("") at the end in case the last line is comment which lead to INCOMPLETE
            scalaInterpret(code + "\nprint(\"\")")
        }
        context.out.flush()
        status
      }
    }
    // reset the java stdout
    System.setOut(originalOut)

    context.out.write("")
    val lastStatus = _interpret(code) match {
      case scala.tools.nsc.interpreter.Results.Success =>
        InterpreterResult.Code.SUCCESS
      case scala.tools.nsc.interpreter.Results.Error =>
        InterpreterResult.Code.ERROR
      case scala.tools.nsc.interpreter.Results.Incomplete =>
        InterpreterResult.Code.INCOMPLETE
    }

    lastStatus match {
      case InterpreterResult.Code.INCOMPLETE => new InterpreterResult(lastStatus, "Incomplete expression")
      case _ => new InterpreterResult(lastStatus)
    }
  }

}

