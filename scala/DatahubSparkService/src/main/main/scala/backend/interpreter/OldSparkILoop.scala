package backend.interpreter

/*
import java.io.{BufferedReader, PrintWriter}
import scala.Predef.{println => _}
import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.tools.nsc.interpreter.shell.{ILoop, ShellConfig}
import scala.util.Properties.{javaVersion, javaVmName, versionString}
*/

/**
 * Scala REPL modified from {@link org.apache.spark.repl.SparkILoop}.
 */

// Todo explore ammonite REPL extension for much more features.
/*
class SparkILoop(in0: BufferedReader, out: PrintWriter)
  extends ILoop(ShellConfig(new GenericRunnerSettings(_ => ())), in0, out) {
  def this() = this(null, new PrintWriter(Console.out, true))

  override protected def internalReplAutorunCode(): Seq[String] = Seq.empty

  /** Print a datahub welcome message */
  override def printWelcome(): Unit = {
    import org.apache.spark.SPARK_VERSION
    echo("""Welcome to
    _____            _             _    _           _
   |  __ \          | |           | |  | |         | |
   | |  | |   __ _  | |_    __ _  | |__| |  _   _  | |__
   | |  | |  / _` | | __|  / _` | |  __  | | | | | | '_ \
   | |__| | | (_| | | |_  | (_| | | |  | | | |_| | | |_) |
   |_____/   \__,_|  \__|  \__,_| |_|  |_|  \__,_| |_.__/
         """)
   /* val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString,
      javaVmName,
      javaVersion
    )
    echo(welcomeMsg) */
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  /** Available commands */
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

  /** Start an interpreter with the given settings.
   *  @return true if successful
   */
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
}

*/
