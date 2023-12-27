package oracle.datahub.spark.service;

import oracle.datahub.spark.backend.interpreter.SparkScalaInnerInterpreter;
import oracle.datahub.spark.backend.scheduler.DriverWatcher;
import oracle.datahub.spark.model.context.ClusterContext;
import oracle.datahub.spark.model.ClusterState;
import oracle.datahub.spark.model.request.CreateClusterRequest;
import oracle.datahub.spark.model.response.CreateClusterResponse;
import oracle.datahub.spark.model.response.GetClusterResponse;
import oracle.datahub.spark.prod.ScalaInterpreter;
import oracle.datahub.spark.prod.SharedSparkContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ClusterManager {
  public static Map<String, ClusterContext> _clusters = new HashMap<>();
  public Response createCluster(CreateClusterRequest request) throws IOException {
    log.info("Creating new cluster");
    //String clusterId = UUID.randomUUID().toString();

    /* This is spark connect
    String standAloneMaster = "spark://siselvan-mac:7077";
    String local = "local[*]";
    SparkSession session = SparkSession
        .builder()
        .appName(request.getName())
        //.config("spark.extraListeners", "sparkmonitor.listener.JupyterSparkMonitorListener")
       // .config("spark.driver.extraClassPath", "/Users/siselvan/github/sparkmonitor/scalalistener/target/scala-2.13/sparkmonitor_2.13-1.0.jar")
        .master(local)
        .getOrCreate();
    try {
      SparkConnectService.start(session.sparkContext());
    } catch (Exception ex) {
      log.error("Exception starting spark connect server ", ex);
    }
    */

    ScalaInterpreter innerInterpreter = new ScalaInterpreter();
    innerInterpreter.startInnerRepl();
    SharedSparkContext sharedSparkContext = innerInterpreter.getSharedSparkContext();
    String clusterId = sharedSparkContext.getSparkSession().sessionUUID();

    ClusterContext clusterContext = ClusterContext
        .builder()
        .id(clusterId)
        .name(request.getName())
        .accessMode(request.getAccessMode())
        .driverNodeType(request.getDriverNodeType())
        .workerNodeType(request.getWorkerNodeType())
        .sparkVersion(request.getSparkVersion())
        .numWorkers(request.getNumWorkers())
        .status(ClusterState.CREATING)
        .parentSparkSession(sharedSparkContext.getSparkSession())
        .build();
    _clusters.put(clusterId, clusterContext);
    log.info("Successfully create cluster {} with id {}", clusterContext, clusterId);
    _clusters.get(clusterId).setStatus(ClusterState.ACTIVE);
    return Response.status(Status.CREATED)
        .entity(CreateClusterResponse
            .builder()
            .clusterId(clusterId)
            .clusterUrl(sharedSparkContext.getSc().uiWebUrl().get())
            .build())
        .build();
  }

  public SparkSession getSparkSession(String clusterId) {
    log.info("Getting cluster info");
    if (!_clusters.containsKey(clusterId)) {
      throw new IllegalArgumentException("Please provide clusterId");
    }
    ClusterContext clusterContext = _clusters.get(clusterId);
    return clusterContext.getParentSparkSession();
  }
}
