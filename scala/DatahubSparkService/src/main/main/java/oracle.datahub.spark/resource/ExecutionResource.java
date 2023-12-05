package oracle.datahub.spark.resource;

import oracle.datahub.spark.model.request.CreateClusterRequest;
import oracle.datahub.spark.model.request.CreateExecutionContextRequest;
import oracle.datahub.spark.service.ClusterManager;
import oracle.datahub.spark.service.ExecutionManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
@Path("/datahub")
public class ExecutionResource {
  private ExecutionManager executionManager;

  public ExecutionResource() {
    executionManager = new ExecutionManager();
  }
  @POST
  @Path("/executionContext/create")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createExecutionContext(CreateExecutionContextRequest request) {
    return executionManager.createExecutionContext(request);
  }
}
