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

package oracle.datahub.spark.backend.interpreter;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interpreter context
 */
public class InterpreterContext {
  private static final ThreadLocal<InterpreterContext> threadIC = new ThreadLocal<>();
  private static final ConcurrentHashMap<Thread, InterpreterContext> allContexts = new ConcurrentHashMap<>();

  public InterpreterOutput out;

  public static InterpreterContext get() {
    return threadIC.get();
  }

  public static void set(InterpreterContext ic) {
    threadIC.set(ic);
    allContexts.put(Thread.currentThread(), ic);
  }

  public static void remove() {
    threadIC.remove();
    allContexts.remove(Thread.currentThread());
  }

  public static ConcurrentHashMap<Thread, InterpreterContext> getAllContexts() {
    return allContexts;
  }

  private String noteId;
  private String noteName;
  private String replName;
  private String paragraphTitle;
  private String paragraphId;
  private String paragraphText;
  private Map<String, Object> config = new HashMap<>();
  private String interpreterClassName;
  private Map<String, Integer> progressMap;
  private Map<String, String> localProperties = new HashMap<>();

  /**
   * Builder class for InterpreterContext
   */
  public static class Builder {
    private InterpreterContext context;

    public Builder() {
      context = new InterpreterContext();
    }

    public Builder setNoteId(String noteId) {
      context.noteId = noteId;
      return this;
    }

    public Builder setNoteName(String noteName) {
      context.noteName = noteName;
      return this;
    }

    public Builder setParagraphId(String paragraphId) {
      context.paragraphId = paragraphId;
      return this;
    }

    public Builder setInterpreterClassName(String intpClassName) {
      context.interpreterClassName = intpClassName;
      return this;
    }


    public Builder setReplName(String replName) {
      context.replName = replName;
      return this;
    }


    public Builder setConfig(Map<String, Object> config) {
      if (config != null) {
        context.config = Maps.newHashMap(config);
      }
      return this;
    }


    public Builder setInterpreterOut(InterpreterOutput out) {
      context.out = out;
      return this;
    }



    public Builder setProgressMap(Map<String, Integer> progressMap) {
      context.progressMap = progressMap;
      return this;
    }

    public Builder setParagraphText(String paragraphText) {
      context.paragraphText = paragraphText;
      return this;
    }

    public Builder setParagraphTitle(String paragraphTitle) {
      context.paragraphTitle = paragraphTitle;
      return this;
    }

    public Builder setLocalProperties(Map<String, String> localProperties) {
      context.localProperties = localProperties;
      return this;
    }

    public InterpreterContext build() {
      return context;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private InterpreterContext() {

  }


  public String getNoteId() {
    return noteId;
  }

  public String getNoteName() {
    return noteName;
  }

  public String getReplName() {
    return replName;
  }

  public String getParagraphId() {
    return paragraphId;
  }

  public void setParagraphId(String paragraphId) {
    this.paragraphId = paragraphId;
  }

  public String getParagraphText() {
    return paragraphText;
  }

  public String getParagraphTitle() {
    return paragraphTitle;
  }

  public Map<String, String> getLocalProperties() {
    return localProperties;
  }

  public String getStringLocalProperty(String key, String defaultValue) {
    return localProperties.getOrDefault(key, defaultValue);
  }

  public int getIntLocalProperty(String key, int defaultValue) {
    return Integer.parseInt(localProperties.getOrDefault(key, defaultValue + ""));
  }

  public long getLongLocalProperty(String key, int defaultValue) {
    return Long.parseLong(localProperties.getOrDefault(key, defaultValue + ""));
  }

  public double getDoubleLocalProperty(String key, double defaultValue) {
    return Double.parseDouble(localProperties.getOrDefault(key, defaultValue + ""));
  }

  public boolean getBooleanLocalProperty(String key, boolean defaultValue) {
    return Boolean.parseBoolean(localProperties.getOrDefault(key, defaultValue + ""));
  }

  public Map<String, Object> getConfig() {
    return config;
  }


  public String getInterpreterClassName() {
    return interpreterClassName;
  }

  public void setInterpreterClassName(String className) {
    this.interpreterClassName = className;
  }

  public InterpreterOutput out() {
    return out;
  }

}
