package com.test;

import com.test.transforms.Transforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class ReadPoemAndSampleOutput {

  public static final String KING_LEAR_POEM = "gs://apache-beam-samples/shakespeare/kinglear.txt";

  public static void main(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline.apply("Read",
      TextIO.read().from(options.getInputFile())
    ).apply("Filter empty lines", Filter.by(StringUtils::isNotEmpty));

    PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);

    lines
      .apply(sample)
      .apply(Flatten.iterables())
      .apply("Log lines", ParDo.of(new Transforms.LogStrings()));

    PCollection<String> words = lines
      .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
      .apply(Filter.by((String word) -> !word.isEmpty()));

    words.apply(sample)
      .apply(Flatten.iterables())
      .apply("Log words", ParDo.of(new Transforms.LogStrings()));

    pipeline.run().waitUntilFinish();
  }

  public interface MyOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);
  }
}
