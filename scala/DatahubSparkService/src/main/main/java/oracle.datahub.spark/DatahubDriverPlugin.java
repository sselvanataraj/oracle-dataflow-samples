package oracle.datahub.spark;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.sql.connect.service.SparkConnectService;

@Slf4j
public class DatahubDriverPlugin implements SparkPlugin {

  private PluginContext executorContext;

  @Override
  public DriverPlugin driverPlugin() {
    DriverPlugin driverPlugin = new DriverPlugin() {
      @Override
      public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        SparkConnectService.start(sc);
        return Collections.emptyMap();
      }
      @Override
      public void shutdown() {
        SparkConnectService.stop(null,null);
      }
    };

    return driverPlugin;
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    ExecutorPlugin executorPlugin = new ExecutorPlugin() {
      @Override
      public void init(PluginContext ctx, Map<String, String> extraConf) {
        log.info("Nothing to do on executor init");
      }
    };
    return executorPlugin;
  }

}
