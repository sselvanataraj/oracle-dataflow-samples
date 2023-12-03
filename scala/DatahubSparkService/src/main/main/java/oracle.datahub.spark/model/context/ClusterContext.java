package oracle.datahub.spark.model.context;

import oracle.datahub.spark.model.ClusterAccessMode;
import oracle.datahub.spark.model.ClusterState;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

@Getter
@Setter
@Builder
@Data
@AllArgsConstructor
public class ClusterContext {
  String id;
  String name;
  ClusterAccessMode accessMode;
  String sparkVersion;
  String driverNodeType;
  String workerNodeType;
  String numWorkers;
  ClusterState status;
  SparkContext sc;
  SparkSession parentSparkSession;
  Set<ExecutionContext> executionsContexts;
}
