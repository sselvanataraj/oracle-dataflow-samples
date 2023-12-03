package oracle.datahub.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum ClusterAccessMode {
  @JsonProperty("single_user")
  SINGLE_USER,
  @JsonProperty("shared_session_isolated")
  SHARED_SESSION_ISOLATED,
  @JsonProperty("shared_no_isolation")
  SHARED_NON_ISOLATED,
  @JsonProperty("shared_fully_isolated")
  SHARED_FULLY_ISOLATED
}
