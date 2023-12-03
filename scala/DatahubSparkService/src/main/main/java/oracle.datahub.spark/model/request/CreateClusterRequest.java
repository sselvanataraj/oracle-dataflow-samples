package oracle.datahub.spark.model.request;

import oracle.datahub.spark.model.ClusterAccessMode;
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
public class CreateClusterRequest {
  String name;
  ClusterAccessMode accessMode;
  String sparkVersion;
  String driverNodeType;
  String workerNodeType;
  String numWorkers;
}
