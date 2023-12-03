/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.datahub.spark.backend.scheduler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
@Getter
@Setter
public abstract class Job<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);
  private static SimpleDateFormat JOB_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

  public enum Status {
    UNKNOWN, READY, PENDING, RUNNING, FINISHED, ERROR, ABORT;
  }

  private String jobName;
  private String id;
  private Date dateCreated;
  transient boolean aborted = false;

  public Job(String jobName) {
    this.jobName = jobName;
    this.id = jobName;
  }


  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;

    if (!(obj instanceof Job))
      return false;

    return ((Job<?>) obj).id.equals(id);
  }

  protected abstract T jobRun() throws Throwable;

  public void run() {
    try {
      jobRun();
    } catch (Throwable e) {
      log.error("Job failed", e);
    }
  }
}


