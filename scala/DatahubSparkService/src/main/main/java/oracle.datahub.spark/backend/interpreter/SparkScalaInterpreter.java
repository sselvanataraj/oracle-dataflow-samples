package oracle.datahub.spark.backend.interpreter;

/*
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import backend.interpreter.ScalaInnerInterpreter;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

@Getter
@Setter

*/

public class SparkScalaInterpreter {
  /*
  protected ScalaInnerInterpreter innerInterpreter;
  protected SparkConf sparkConf;
  protected SparkContext sc;
  protected JavaSparkContext jsc;
  protected SQLContext sqlContext;
  protected SparkSession sparkSession;
  protected String sparkVersion;
  protected String sparkUrl;
  protected List<String> depFiles;
  private static File scalaShellOutputDir;

  public SparkScalaInterpreter() throws IOException {
    scalaShellOutputDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "spark")
        .toFile();
    scalaShellOutputDir.deleteOnExit();
    this.sparkConf = new SparkConf();
   // this.sparkConf.set("spark.master", "local[2]");
   // this.sparkConf.set("spark.repl.classdir","/Users/siselvan/github/oracle-dataflow-samples/scala/DatahubSparkService/src/main/main/scala/backend/interpreter/");
    List<URL> urls = new ArrayList<>();
    URLClassLoader scalaInterpreterClassLoader = new URLClassLoader(urls.toArray(new URL[0]),
        Thread.currentThread().getContextClassLoader());
    this.innerInterpreter = new ScalaInnerInterpreter(sparkConf,scalaInterpreterClassLoader,scalaShellOutputDir);
    innerInterpreter.createSparkILoop();
    this.depFiles = new ArrayList<>();
  }

  private List<String> getUserFiles() {
    return depFiles.stream()
        .filter(f -> f.endsWith(".jar"))
        .collect(Collectors.toList());
  }

  public void createSparkContext() {
    SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
   // sparkSession = builder.getOrCreate();
    System.out.println("Created Parent Spark session");
    // shared spark context
    sc = sparkSession.sparkContext();
    getUserFiles().forEach(file -> sc.addFile(file));
    if (sc.uiWebUrl().isDefined()) {
      sparkUrl = sc.uiWebUrl().get();
    }
    sqlContext = sparkSession.sqlContext();
    jsc = JavaSparkContext.fromSparkContext(sc);
    sparkVersion = sc.version();
    innerInterpreter.bind("spark", sparkSession.getClass().getCanonicalName(), sparkSession, Lists.newArrayList("@transient"));
    innerInterpreter.bind("sc", "org.apache.spark.SparkContext", sc, Lists.newArrayList("@transient"));
    innerInterpreter.bind("sqlContext", "org.apache.spark.sql.SQLContext", sqlContext, Lists.newArrayList("@transient"));

    innerInterpreter.scalaInterpretQuietly("import org.apache.spark.SparkContext._");
    innerInterpreter.scalaInterpretQuietly("import spark.implicits._");
    innerInterpreter.scalaInterpretQuietly("import sqlContext.implicits._");
    innerInterpreter. scalaInterpretQuietly("import spark.sql");
    innerInterpreter.scalaInterpretQuietly("import org.apache.spark.sql.functions._");
    // print empty string otherwise the last statement's output of this method
    // (aka. import org.apache.spark.sql.functions._) will mix with the output of user code
    innerInterpreter. scalaInterpretQuietly("print(\"\")");
    System.out.println("Thread Context " + Thread.currentThread().getId());
  }
  */
}
