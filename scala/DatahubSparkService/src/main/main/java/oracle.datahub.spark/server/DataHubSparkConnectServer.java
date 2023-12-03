package oracle.datahub.spark.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService;

@Slf4j
public class DataHubSparkConnectServer  {
  public static void main(String[] args) throws InterruptedException {
    SparkSession session = SparkSession.builder().master("spark://siselvan-mac:7077").getOrCreate();
    try {
      SparkConnectService.start(session.sparkContext());
    } catch (Exception ex) {
      System.exit(-1);
    }
    Thread.currentThread().join();
    log.info(String.format("Application started.%nStop the application using CTRL+C"));
  }
}
