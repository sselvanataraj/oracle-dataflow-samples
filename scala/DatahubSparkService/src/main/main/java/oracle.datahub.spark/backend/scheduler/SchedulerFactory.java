package oracle.datahub.spark.backend.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchedulerFactory {
  private static final String SCHEDULER_EXECUTOR_NAME = "SchedulerFactory";

  protected ExecutorService executor;
  protected Map<String, Scheduler> schedulers = new HashMap<>();

  private static final class InstanceHolder {
    private static final SchedulerFactory INSTANCE = new SchedulerFactory();
  }

  public static SchedulerFactory singleton() {
    return InstanceHolder.INSTANCE;
  }

  private SchedulerFactory() {
    int threadPoolSize = 5;
    executor = ExecutorFactory.singleton().createOrGet(SCHEDULER_EXECUTOR_NAME, threadPoolSize);
  }

  public void destroy() {
    log.info("Destroy all executors");
    synchronized (schedulers) {
      // stop all child thread of schedulers
      for (Entry<String, Scheduler> scheduler : schedulers.entrySet()) {
        log.info("Stopping Scheduler {}", scheduler.getKey());
        scheduler.getValue().stop();
      }
      schedulers.clear();
    }
    ExecutorUtil.softShutdown("SchedulerFactoryExecutor", executor, 60, TimeUnit.SECONDS);
  }

  public Scheduler createOrGetFIFOScheduler(String name) {
    synchronized (schedulers) {
      if (!schedulers.containsKey(name)) {
        log.info("Create FIFOScheduler: {}", name);
        FIFOScheduler s = new FIFOScheduler(name);
        schedulers.put(name, s);
        executor.execute(s);
      }
      return schedulers.get(name);
    }
  }

  public Scheduler createOrGetParallelScheduler(String name, int maxConcurrency) {
    synchronized (schedulers) {
      if (!schedulers.containsKey(name)) {
        log.info("Create ParallelScheduler: {} with maxConcurrency: {}", name, maxConcurrency);
        ParallelScheduler s = new ParallelScheduler(name, maxConcurrency);
        schedulers.put(name, s);
        executor.execute(s);
      }
      return schedulers.get(name);
    }
  }


  public Scheduler createOrGetScheduler(Scheduler scheduler) {
    synchronized (schedulers) {
      if (!schedulers.containsKey(scheduler.getName())) {
        schedulers.put(scheduler.getName(), scheduler);
        executor.execute(scheduler);
      }
      return schedulers.get(scheduler.getName());
    }
  }

  public void removeScheduler(String name) {
    synchronized (schedulers) {
      log.info("Remove scheduler: {}", name);
      Scheduler s = schedulers.remove(name);
      if (s != null) {
        s.stop();
      }
    }
  }

  public ExecutorService getExecutor() {
    return executor;
  }
}
