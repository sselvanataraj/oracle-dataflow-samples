package oracle.datahub.spark.model.context;

import oracle.datahub.spark.backend.interpreter.InterpreterContext;
import oracle.datahub.spark.backend.interpreter.InterpreterResult;
import oracle.datahub.spark.backend.interpreter.SparkScalaInnerInterpreter;
import oracle.datahub.spark.backend.scheduler.Job;
import java.io.IOException;
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
public class StatementContext extends Job<InterpreterResult> {
  String id;
  String executionContextId;
  String clusterContextId;
  String lang; // can be different from ExecutionContext language
  String code;
  String result;

  protected InterpreterResult jobRun() throws IOException {
    SparkScalaInnerInterpreter interpreter = new SparkScalaInnerInterpreter();
    InterpreterResult ret = interpreter.internalInterpret(code, getInterpreterContext());
    return ret;
  }

  private InterpreterContext getInterpreterContext() {
    return InterpreterContext
        .builder()
        .setNoteId(executionContextId)
        .setParagraphId(id)
        .setReplName(lang)
        .setParagraphText(code)
        .build();
  }
}
