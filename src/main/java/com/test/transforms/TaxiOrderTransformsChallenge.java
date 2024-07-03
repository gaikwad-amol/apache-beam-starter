package com.test.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;


/**
 You are provided with a PCollection created from the array of taxi order prices in a csv file.
 Your task is to find how many orders are below $15 and how many are equal to or above $15.
 Return it as a map structure (key-value), make above or below the key, and the total dollar value (sum) of orders - the value.
 Although there are many ways to do this, try using another transformation presented in this module.
 **/
@Slf4j
public class TaxiOrderTransformsChallenge {

  public interface MyOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv")
    String getInputFile();

    void setInputFile(String value);

//    @Description("Path of the file to write to")
//    @Validation.Required
//    String getOutput();
//
//    void setOutput(String value);
  }

  public static void main(String[] args) {
    log.info("running the pipeline with args {}", Arrays.toString(args));
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

    Pipeline pipeline = Pipeline.create();
    PCollection<String> rows = pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()));
    PCollection<Double> costs = rows.apply(ParDo.of(new ExtractTaxiCostFn()));
    PCollection<Double> costsGreaterThan15 = costs.apply(Filter.greaterThanEq(15.0));
    PCollection<Double> costsLessThan15 = costs.apply(Filter.lessThan(15.0));
    PCollection<KV<String, Double>> above = costsGreaterThan15.apply(Sum.doublesGlobally()).apply(WithKeys.of("above"));
    PCollection<KV<String, Double>> below = costsLessThan15.apply(Sum.doublesGlobally()).apply(WithKeys.of("below"));

    above.apply(ParDo.of(new LogOutput<>()));
    below.apply(ParDo.of(new LogOutput<>()));
    pipeline.run().waitUntilFinish();
  }

  static class ExtractTaxiCostFn extends DoFn<String, Double> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] split = c.element().split(",");
      double value = 0.0;
      if (split.length > 16) {
        try {
          value = Double.parseDouble(split[16]);
        }catch (NumberFormatException exception) {
          value = 0.0;
        }
      }
      c.output(value);
    }

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
