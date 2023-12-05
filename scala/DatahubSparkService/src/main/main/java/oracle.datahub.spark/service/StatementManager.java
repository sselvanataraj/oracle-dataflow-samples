package oracle.datahub.spark.service;

import oracle.datahub.spark.backend.interpreter.InterpreterContext;
import oracle.datahub.spark.backend.scheduler.Scheduler;
import oracle.datahub.spark.model.context.ClusterContext;
import oracle.datahub.spark.model.context.ExecutionContext;
import oracle.datahub.spark.model.context.StatementContext;
import oracle.datahub.spark.model.request.CreateExecutionContextRequest;
import oracle.datahub.spark.model.request.ExecuteStatementRequest;
import oracle.datahub.spark.model.response.CreateExecutionContextResponse;
import oracle.datahub.spark.model.response.ExecuteStatementResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class StatementManager {
  public static Map<String, StatementContext>  statements = new HashMap<>();
  public Response executeStatement (ExecuteStatementRequest request) {
    log.info("Executing statement");
    ExecutionContext executionContext = ExecutionManager
        .executionContexts.get(request.getContextId());
    String statementId = UUID.randomUUID().toString();
    StatementContext statementContext = StatementContext
        .builder()
        .clusterContextId(executionContext.getClusterContextId())
        .code(request.getCode())
        .executionContextId(request.getContextId())
        .lang(request.getLang())
        .build();


    /*
    Scheduler scheduler = executionContext.getScheduler();
    scheduler.submit(statementContext);
    */
    statements.put(statementId, statementContext);
    return Response.status(Status.CREATED)
        .entity(ExecuteStatementResponse
            .builder()
            .statementId(statementId)
            .build())
        .build();
  }
}
