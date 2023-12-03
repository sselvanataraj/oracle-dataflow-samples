package oracle.datahub.spark.service;

import oracle.datahub.spark.model.context.ExecutionContext;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutionManager {
  ConcurrentHashMap<String, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

}
