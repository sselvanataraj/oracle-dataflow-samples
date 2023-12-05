package oracle.datahub.spark.service;

import oracle.datahub.spark.backend.scheduler.FIFOScheduler;
import oracle.datahub.spark.backend.scheduler.Scheduler;
import oracle.datahub.spark.model.ClusterState;
import oracle.datahub.spark.model.context.ClusterContext;
import oracle.datahub.spark.model.context.ExecutionContext;
import oracle.datahub.spark.model.request.CreateClusterRequest;
import oracle.datahub.spark.model.request.CreateExecutionContextRequest;
import oracle.datahub.spark.model.response.CreateClusterResponse;
import oracle.datahub.spark.model.response.CreateExecutionContextResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService;

@Slf4j
@Getter
@Setter
public class ExecutionManager {
 public static Map<String, ExecutionContext> executionContexts = new HashMap<>();
  public Response createExecutionContext(CreateExecutionContextRequest request) {
    log.info("Creating new execution context");
    String executionId = UUID.randomUUID().toString();
    ClusterContext clusterContext  = ClusterManager._clusters.get(request.getClusterId());
    SparkSession spark = clusterContext.getParentSparkSession().newSession();
    Scheduler scheduler = new FIFOScheduler(executionId);
    ExecutionContext executionContext = ExecutionContext.builder()
        .clusterContextId(request.getClusterId())
        .id(executionId)
        .lang(request.getLang())
        .spark(spark)
        .build();
    executionContexts.put(executionId,executionContext);

    return Response.status(Status.CREATED)
        .entity(CreateExecutionContextResponse
            .builder()
            .contextId(executionId)
            .sparkSession(spark)
            .build())
        .build();
  }
}
