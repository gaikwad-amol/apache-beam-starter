package com.test;

import com.test.transforms.Transforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class InMemoryPCollection {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> strings = pipeline
      .apply("CreateStrings",
        Create.of("To", "be", "or", "not", "to", "be", "that", "is", "the", "question")
      )
      .apply("LogStrings", ParDo.of(new Transforms.LogStrings()));
    PCollection<Integer> integers = pipeline
      .apply("CreateIntegers",
        Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      )
      .apply("LogIntegers", ParDo.of(new Transforms.LogIntegers()));

    pipeline.run().waitUntilFinish();
  }
}
