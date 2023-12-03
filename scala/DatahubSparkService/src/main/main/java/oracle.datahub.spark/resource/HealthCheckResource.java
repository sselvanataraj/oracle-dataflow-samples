package oracle.datahub.spark.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

@Path("/datahub")
@Slf4j
public class HealthCheckResource {
  @GET
  @Path("/healthcheck")
  @Produces(MediaType.APPLICATION_JSON)
  public Response healthCheck() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      // create a JSON string
      // ObjectNode json = mapper.createObjectNode();
      // json.put("status", "Hello from DataHub !!!");
      return Response.ok("Hello from DataHub !!!").build();
    } catch (Exception ex) {
      log.error("Exception ", ex);
    }
    return Response.status(Status.BAD_REQUEST).build();
  }
}
