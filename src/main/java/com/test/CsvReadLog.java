package com.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

@Slf4j
public class CsvReadLog {

  public interface MyOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("src/main/resources/TaxiOrderData.csv")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }
  public static void main(String[] args) {
    log.info("running the pipeline with args {}", Arrays.toString(args));
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

    Pipeline pipeline = Pipeline.create();
    PCollection<String> rows = pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()));

    rows.apply("Log", ParDo.of(new LogOutput<String>()));
    pipeline.run().waitUntilFinish();
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private final String prefix;

    LogOutput() {
      this.prefix = "Processing element";
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      log.info("{}: {}", prefix, c.element());
    }
  }
}
