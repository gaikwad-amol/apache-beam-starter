package com.test.transforms.coretransformers;

import com.test.transforms.LogUtilTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * You are given a csv file with the players' records, which you need to share with regex.
 * It is necessary to "Sum up" by username using Combine, each player's point must be combined(+)
 */
public class Challenge2 {

  private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    String input = "gs://apache-beam-samples/game/small/gaming_data.csv";
    PCollection<String> data = pipeline.apply(TextIO.read().from(input));
    PCollection<KV<String, Integer>> playersScore = extractPlayerInfo(data);
    PCollection<KV<String, Integer>> combinedScore = combineScore(playersScore);
    combinedScore.apply(ParDo.of(new LogUtilTransforms.LogOutput<>()));
    pipeline.run().waitUntilFinish();
  }

  private static PCollection<KV<String, Integer>> combineScore(PCollection<KV<String, Integer>> playersScore) {
    return playersScore.apply(Combine.perKey(new Combine.BinaryCombineIntegerFn() {
      @Override
      public int apply(int left, int right) {
        return left + right;
      }

      @Override
      public int identity() {
        return 0;
      }
    }));
  }

  static PCollection<KV<String, Integer>> extractPlayerInfo(PCollection<String> row) {
    return row.apply(ParDo.of(new DoFn<String, KV<String, Integer>>() {
      @ProcessElement
      public void processElement(@Element String line, OutputReceiver<KV<String, Integer>> outputReceiver) {
        String[] words = line.split(REGEX_FOR_CSV);
        outputReceiver.output(KV.of(words[1], Integer.parseInt(words[2])));
      }
    }));

  }
}
