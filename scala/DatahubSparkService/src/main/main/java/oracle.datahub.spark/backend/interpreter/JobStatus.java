package oracle.datahub.spark.backend.interpreter;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum JobStatus {
  @JsonProperty("RUNNING")
  RUNNING,
  @JsonProperty("FAILED")
  FAILED,
  @JsonProperty("SUCCEEDED")
  SUCCEEDED
}
