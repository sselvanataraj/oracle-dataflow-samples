package oracle.datahub.spark.backend.interpreter;

import java.io.Serializable;

public class JobResult implements Serializable {

  JobStatus jobStatus;
  String result;

}
