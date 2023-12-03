package oracle.datahub.spark.backend.scheduler;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class SchedulerThreadFactory implements ThreadFactory {

  private String namePrefix;
  private AtomicLong count = new AtomicLong(1);

  public SchedulerThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    thread.setName(namePrefix + count.getAndIncrement());
    return thread;
  }
}
