package oracle.datahub.spark.model.context;

import oracle.datahub.spark.backend.interpreter.InterpreterContext;
import oracle.datahub.spark.backend.interpreter.SparkScalaInnerInterpreter;
import oracle.datahub.spark.backend.scheduler.Scheduler;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
@Getter
@Setter
@Builder
@AllArgsConstructor

/**
 * 1:many to {@link ClusterContext}
 */
public class ExecutionContext {
  String id;
  String clusterContextId;
  String lang; // Default Language
  SparkSession spark;
  InterpreterContext interpreterContext;
  Scheduler scheduler;
}
