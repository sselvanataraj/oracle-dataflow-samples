package oracle.datahub.spark.server;

import oracle.datahub.spark.resource.ClusterResource;
import oracle.datahub.spark.resource.HealthCheckResource;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

@Slf4j
public class DataHubServer {

  private static final String HOST = "localhost";
  private static final String PORT = "9090";
  private static final String BASE_URI = "http://" + HOST + ":" + PORT + "/";

  public static Server startServer() {
    ResourceConfig config = new ResourceConfig();
    config.register(HealthCheckResource.class);
    config.register(ClusterResource.class);
    return JettyHttpContainerFactory.createServer(URI.create(BASE_URI), config);
  }

  public static void main(String[] args) {
    try {
      final Server server = startServer();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          log.info("Shutting down the application...");
          server.stop();
          log.info("Done, exit.");
        } catch (Exception e) {
          log.error("Exception shutting down ", e);
        }
      }));

      log.info(String.format("Application started.%nStop the application using CTRL+C"));
      Thread.currentThread().join();
    } catch (InterruptedException ex) {
     log.error("Interrupted Exception ", ex);
    }
  }
}

