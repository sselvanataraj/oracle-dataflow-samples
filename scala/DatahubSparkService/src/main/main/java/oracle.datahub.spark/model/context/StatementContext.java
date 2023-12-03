package oracle.datahub.spark.model.context;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 1:many to {@link ExecutionContext}
 */
@Slf4j
@Getter
@Setter
@Builder
public class StatementContext  {
  String id;
  String executionContextId;
  String clusterContextId;
  String lang; // can be different from ExecutionContext language
  String code;
  String result;

  protected String jobRun() {
    return null;
  }
}
