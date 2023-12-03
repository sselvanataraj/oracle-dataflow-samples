package oracle.datahub.spark.backend.scheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Interface for scheduler. Scheduler is used for manage the lifecycle of job.
 * Including query, submit and cancel job.
 *
 * Scheduler can run both in Zeppelin Server and Interpreter Process. e.g. RemoveScheduler run
 * in Zeppelin Server side while FIFOScheduler run in Interpreter Process.
 */
public interface Scheduler extends Runnable {

  String getName();

  List<Job<?>> getAllJobs();

  Job<?> getJob(String jobId);

  void submit(Job<?> job);

  Job<?> cancel(String jobId);

  void stop();

  void stop(int stopTimeoutVal, TimeUnit stopTimeoutUnit);

}
