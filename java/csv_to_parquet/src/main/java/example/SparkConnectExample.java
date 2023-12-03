package example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkConnectExample {

  public static void main(String[] args) {
    String clusterId = "default";
    SparkSession spark = new DataHubSparkSession(clusterId).getOrCreate();
    String path = "/Users/siselvan/github/oracle-dataflow-samples/java/csv_to_parquet/src/main/resources/people.csv";
    Dataset<Row> df = spark.read().csv(path);
    df.createOrReplaceTempView("table");
    df.createOrReplaceGlobalTempView("table");
    spark.table("table").show();

    // 1. Change in code doesn't need to upload the jar
    /*
      mvn package
      java --add-opens=java.base/java.nio=ALL-UNNAMED -cp target/csv_to_parquet-1.0-SNAPSHOT-jar-with-dependencies.jar example.SparkConnectExample
     */
    spark.table("table").filter(df.col("_c0").$eq$eq$eq("Jorge")).show();

    // 2. Dependency Isolation and add dependencies at run time
    spark.addArtifact("/Users/siselvan/github/sparkmonitor/scalalistener/target/scala-2.13/sparkmonitor_2.13-1.0.jar");

    // 3. Stopping Session Isolation
    spark.stop();

    // Remote debugging
  }
}
