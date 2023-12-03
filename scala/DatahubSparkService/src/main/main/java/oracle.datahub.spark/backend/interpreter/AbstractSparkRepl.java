package oracle.datahub.spark.backend.interpreter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractSparkRepl {
  /*
  private static final AtomicInteger EXECUTION_CONTEXT_NUMBER = new AtomicInteger(0);
  SparkConf sparkConf;
  SparkContext sc;
  SparkSession sparkSession;

  public AbstractSparkRepl(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  public void init() {
    System.setProperty("scala.repl.name.line", ("$line" + this.hashCode()).replace('-', '0'));
    EXECUTION_CONTEXT_NUMBER.incrementAndGet();
    createSparkILoop();
    createSharedSparkContext();
  }

  //public abstract JobResult execute(String code);

  public abstract void destroy();

  public abstract void createSparkILoop();

  public abstract void bind(String name,
      String tpe,
      Object value,
      List<String> modifier);

  // public abstract void scalaInterpretQuietly(String code);

  public abstract ClassLoader getScalaShellClassLoader();

  private void createSharedSparkContext() {
   sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
   sc = sparkSession.sparkContext();

   bind("spark", sparkSession.getClass().getCanonicalName(), sparkSession, Lists.newArrayList("@transient"));
   bind("sc", "org.apache.spark.SparkContext", sc, Lists.newArrayList("@transient"));

   /*
    scalaInterpretQuietly("import org.apache.spark.SparkContext._");
    scalaInterpretQuietly("import spark.implicits._");
    scalaInterpretQuietly("import sqlContext.implicits._");
    scalaInterpretQuietly("import spark.sql");
    scalaInterpretQuietly("import org.apache.spark.sql.functions._");
    // print empty string otherwise the last statement's output of this method
    // (aka. import org.apache.spark.sql.functions._) will mix with the output of user code
    scalaInterpretQuietly("print(\"\")")
    }
   */
}
