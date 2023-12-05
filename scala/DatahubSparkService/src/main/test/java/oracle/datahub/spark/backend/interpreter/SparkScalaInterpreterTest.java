package oracle.datahub.spark.backend.interpreter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Console;

class SparkScalaInterpreterTest {

  public static void main(String[] args) throws IOException {
    SparkScalaInnerInterpreter interpreter = new SparkScalaInnerInterpreter();
    interpreter.createSparkContext();
    System.out.println(interpreter.sc.version());
    interpreter.innerInterpreter.scalaInterpret("2+2");
    interpreter.innerInterpreter.scalaInterpret("val path=\"/Users/siselvan/github/oracle-dataflow-samples/java/csv_to_parquet/src/main/resources/people.csv\"");
    interpreter.innerInterpreter.scalaInterpret("val df=spark.read.csv(path)");
    interpreter.innerInterpreter.scalaInterpret("df.show()");
    //String path = "/Users/siselvan/github/oracle-dataflow-samples/java/csv_to_parquet/src/main/resources/people.csv";
    //Dataset<Row> df = interpreter.sparkSession.read().csv(path);
    System.out.println(interpreter.sc.uiWebUrl());
    interpreter.innerInterpreter.scalaInterpret("df.show()");
    interpreter.sc.stop();
  }
}