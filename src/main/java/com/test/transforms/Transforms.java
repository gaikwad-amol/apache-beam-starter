package com.test.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class Transforms {

  public static class LogStrings extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      log.info("Processing word: {}", c.element());
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
}


