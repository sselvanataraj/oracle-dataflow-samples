package oracle.datahub.spark.resource;

import oracle.datahub.spark.model.request.CreateClusterRequest;
import oracle.datahub.spark.service.ClusterManager;
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
public class ClusterResource {
  private ClusterManager clusterManager;

  public ClusterResource() {
    clusterManager = new ClusterManager();
  }
  @POST
  @Path("/clusters/create")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createCluster(CreateClusterRequest request) throws IOException, InterruptedException {
    return clusterManager.createCluster(request);
  }

  @GET
  @Path("/clusters/{clusterId}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getSparkSession(@NonNull @PathParam("clusterId") String clusterId) {
    try {
      SparkSession sparkSession = clusterManager.getSparkSession(clusterId);
      return Response.ok(sparkSession).build();
    } catch (Exception ex) {
      log.info("Failed to get cluster information",ex);
    }
    return Response.status(Status.BAD_REQUEST).build();
  }
}
