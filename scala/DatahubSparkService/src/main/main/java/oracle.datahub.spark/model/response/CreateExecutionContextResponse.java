package oracle.datahub.spark.model.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.SparkSession;

@Data
@Builder
@AllArgsConstructor
public class CreateExecutionContextResponse {
  String contextId;
  SparkSession sparkSession;
}
