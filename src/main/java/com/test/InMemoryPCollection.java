package com.test;

import com.test.transforms.LogUtilTransforms;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class InMemoryPCollection {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // Logging all the strings and the integers
    PCollection<String> strings = pipeline
      .apply("CreateStrings",
        Create.of("To", "be", "or", "not", "to", "be", "that", "is", "the", "question")
      )
      .apply("LogStrings", ParDo.of(new LogUtilTransforms.LogStrings()));

    PCollection<Integer> integers = pipeline
      .apply("CreateIntegers",
        Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      )
      .apply("LogIntegers", ParDo.of(new LogUtilTransforms.LogIntegers()));

    //-----------------------
    // Sum global example
    PCollection<Integer> numbers = pipeline
      .apply("CreateIntegers",
        Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      ).apply(Sum.integersGlobally())
      .apply("LogIntegers", ParDo.of(new LogUtilTransforms.LogIntegers()));

    //-----------------------
    // Sum per key example
    PCollection<KV<String, Integer>> input = pipeline.apply(
      Create.of(KV.of("🥕", 3),
        KV.of("🥕", 2),
        KV.of("🍆", 1),
        KV.of("🍅", 4),
        KV.of("🍅", 5),
        KV.of("🍅", 3)));
    PCollection<KV<String, Integer>> sumPerKey = input.apply(Sum.integersPerKey())
      .apply(ParDo.of(new LogOutput<>()));

    //-----------------------

    pipeline.run().waitUntilFinish();
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private String prefix;

    LogOutput() {
      this.prefix = "Processing element";
    }

    LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      log.info(prefix + ": {}", c.element());
    }
  }
}
