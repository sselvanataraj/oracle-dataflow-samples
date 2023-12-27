package oracle.datahub.spark.prod;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * One SharedSparkContext per Cluster.
 */
public class SharedSparkContext {

  private static volatile SharedSparkContext instance;
  private static Object mutex = new Object();
  private static volatile SparkContext sc;
  private static volatile SQLContext sqlContext;
  private static volatile SparkSession spark;

  private SharedSparkContext()   {

  }

  public SparkContext getSc() {
    return sc;
  }

  public SQLContext getSqlContext() {
    return sqlContext;
  }

  public SparkSession getSparkSession() {
    return spark;
  }

  public static SharedSparkContext getInstance(SparkConf conf) {
    SharedSparkContext sharedSparkContext = instance;
    if (sharedSparkContext == null) {
      synchronized (mutex) {
        sharedSparkContext = instance;
        if (sharedSparkContext == null) {
          sc = new SparkContext(conf);
          sqlContext = new SQLContext(sc);
          spark = new SparkSession(sc);
          instance = sharedSparkContext = new SharedSparkContext();
        }
      }
    }
    return sharedSparkContext;
  }

}
