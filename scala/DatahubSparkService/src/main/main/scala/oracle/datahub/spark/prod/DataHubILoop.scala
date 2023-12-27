package oracle.datahub.spark.prod

import java.io.{BufferedReader, PrintWriter}
import scala.tools.nsc.interpreter.shell.{ILoop, ShellConfig}
import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class DataHubILoop(in0: BufferedReader, out: PrintWriter)
  extends ILoop(ShellConfig(new GenericRunnerSettings(_ => ())), in0, out) {

  def this() = this(null, new PrintWriter(Console.out, true))

  override def printWelcome(): Unit = {
    echo(
      """Welcome to
  _____            _             _    _           _
 |  __ \          | |           | |  | |         | |
 | |  | |   __ _  | |_    __ _  | |__| |  _   _  | |__
 | |  | |  / _` | | __|  / _` | |  __  | | | | | | '_ \
 | |__| | | (_| | | |_  | (_| | | |  | | | |_| | | |_) |
 |_____/   \__,_|  \__|  \__,_| |_|  |_|  \__,_| |_.__/  version %s
       """.format("2.0.0"))

    val welcomeMsg = String.format("Using Scala %s (%s, Java %s)",
      versionString,
      javaVmName,
      javaVersion
    )
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  override def commands: List[LoopCommand] = standardCommands

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    echo(
      "Note that after :reset, state of SparkSession and SparkContext is unchanged."
    )
  }

  override def replay(): Unit = {
    super.replay()
  }

  override def run(interpreterSettings: Settings): Boolean = {
    createInterpreter(interpreterSettings)
    intp.reporter.withoutPrintingResults(intp.withSuppressedSettings {
      intp.initializeCompiler()
      if (intp.reporter.hasErrors) {
        echo("Interpreter encountered errors during initialization!")
        throw new InterruptedException
      }
    })
    true
  }
  override protected def internalReplAutorunCode(): Seq[String] =
    Seq.empty
}
