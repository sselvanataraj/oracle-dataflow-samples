package oracle.datahub.spark.resource;

import oracle.datahub.spark.model.request.ExecuteStatementRequest;
import oracle.datahub.spark.service.ExecutionManager;
import oracle.datahub.spark.service.StatementManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementResource {
  private StatementManager statementManager;
  private ExecutionManager executionManager;

  public void StatementManager() {
    statementManager = new StatementManager();
  }
  @POST
  @Path("/statement/execute")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response execute(ExecuteStatementRequest request) {
    return statementManager.executeStatement(request);
  }
}
