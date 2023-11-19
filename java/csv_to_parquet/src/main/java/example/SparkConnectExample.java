package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkConnectExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().remote("sc://localhost")
        .appName("Execute spark code in remote and debug").build();
    String path = "/Users/siselvan/github/oracle-dataflow-samples/java/csv_to_parquet/src/main/resources/people.csv";
    Dataset<Row> df = spark.read().csv(path);
    df.show();
  }
}
