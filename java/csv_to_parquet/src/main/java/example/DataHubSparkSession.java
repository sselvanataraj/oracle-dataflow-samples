package example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.client.SparkConnectClient;
public class DataHubSparkSession {

  private SparkSession sparkSession;
  public  DataHubSparkSession(String clusterId) {
    this.sparkSession = SparkSession
        .builder()
        .remote("sc://localhost")
        .client(SparkConnectClient.builder().userId("java").build())
        .appName("Remote Java Code")
        .getOrCreate();
  }

  public SparkSession getOrCreate() {
   return sparkSession;
  }
}
