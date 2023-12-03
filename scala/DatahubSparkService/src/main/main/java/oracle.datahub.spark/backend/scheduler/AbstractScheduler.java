package oracle.datahub.spark.backend.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract class for scheduler implementation. Implementor just need to implement method
 * runJobInScheduler.
 */
@Slf4j
public abstract class AbstractScheduler implements Scheduler {


  protected final String name;
  protected volatile boolean terminate = false;
  protected BlockingQueue<Job<?>> queue = new LinkedBlockingQueue<>();
  protected Map<String, Job<?>> jobs = new ConcurrentHashMap<>();
  private Thread schedulerThread;

  public AbstractScheduler(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public List<Job<?>> getAllJobs() {
    return new ArrayList<>(jobs.values());
  }

  @Override
  public Job<?> getJob(String jobId) {
    return jobs.get(jobId);
  }

  @Override
  public void submit(Job<?> job) {
    try {
      queue.put(job);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format("Unable to submit job %s", job.getId()), e);
    }
    jobs.put(job.getId(), job);
  }

  @Override
  public Job<?> cancel(String jobId) {
    Job<?> job = jobs.remove(jobId);
    return job;
  }

  @Override
  public void run() {
    schedulerThread = Thread.currentThread();
    while (!terminate && !schedulerThread.isInterrupted()) {
      Job<?> runningJob = null;
      try {
        runningJob = queue.take();
      } catch (InterruptedException e) {
        log.warn("{} is interrupted", getClass().getSimpleName());
        // Restore interrupted state...
        Thread.currentThread().interrupt();
        break;
      }

      runJobInScheduler(runningJob);
    }
    stop();
  }

  public abstract void runJobInScheduler(Job<?> job);

  @Override
  public void stop() {
    terminate = true;
    for (Job<?> job : queue) {
      job.aborted = true;
    }
    if (schedulerThread != null) {
      schedulerThread.interrupt();
    }
  }

  /**
   * This is the logic of running job.
   * Subclass can use this method and can customize where and when to run this method.
   *
   * @param runningJob
   */
  protected void runJob(Job<?> runningJob) {
    log.info("Job {} started by scheduler {}", runningJob.getId(), name);
    runningJob.run();
    log.info("Job {} finished by scheduler {}", runningJob.getId(), name);
    // reset aborted flag to allow retry
    runningJob.aborted = false;
    jobs.remove(runningJob.getId());
  }
}
