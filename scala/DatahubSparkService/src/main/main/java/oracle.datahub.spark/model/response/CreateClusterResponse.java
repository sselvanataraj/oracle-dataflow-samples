package oracle.datahub.spark.model.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder
@AllArgsConstructor
public class CreateClusterResponse {
  String clusterId;
  String clusterUrl;
}
