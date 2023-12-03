package oracle.datahub.spark.model.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Data
@AllArgsConstructor
@Builder
public class CreateExecutionContextRequest {
  String clusterId;
  String lang;
}
