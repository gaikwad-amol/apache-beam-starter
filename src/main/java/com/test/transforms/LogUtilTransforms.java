package com.test.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class LogUtilTransforms {

  public static class LogStrings extends DoFn<String, String> {
    private final String prefix;

    public LogStrings() {
      this.prefix = "Processing word";
    }

    public LogStrings(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      log.info("{}: {}", prefix, c.element());
      c.output(c.element());
    }
  }

  public static class LogIntegers extends DoFn<Integer, Integer> {

    private final String prefix;

    public LogIntegers() {
      this.prefix = "Processing element";
    }

    public LogIntegers(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      log.info(" {}: {}", prefix, c.element());
      c.output(c.element());
    }
  }

  public static class LogOutput<T> extends DoFn<T, T> {
    private final String prefix;

    public LogOutput() {
      this.prefix = "Processing element";
    }

    LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      log.info(prefix + ": {}", c.element());
    }
  }
}


