package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkConnectExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().remote("sc://localhost").appName("IDE").build();
    Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
    df.show();
  }
}
