package oracle.datahub.spark.backend.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel scheduler runs submitted job concurrently.
 */
public class ParallelScheduler extends AbstractScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelScheduler.class);

  private ExecutorService executor;

  ParallelScheduler(String name, int maxConcurrency) {
    super(name);
    this.executor = Executors.newFixedThreadPool(maxConcurrency,
        new SchedulerThreadFactory("ParallelScheduler-Worker-"));
  }

  @Override
  public void runJobInScheduler(final Job<?> runningJob) {
    // submit this job to a FixedThreadPool so that at most maxConcurrencyJobs running
    executor.execute(() -> runJob(runningJob));
  }

  @Override
  public void stop() {
    stop(2, TimeUnit.MINUTES);
  }

  @Override
  public void stop(int stopTimeoutVal, TimeUnit stopTimeoutUnit) {
    super.stop();
    ExecutorUtil.softShutdown(name, executor, stopTimeoutVal, stopTimeoutUnit);
  }
}
