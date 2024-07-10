package com.test.transforms.coretransformers;

import com.test.transforms.LogUtilTransforms;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;

/**
 * Core LogUtilTransforms motivating challenge-1
 * You are given the works of Shakespeare "kinglear" it will be divided into words and filtered.
 * You need to divide these words into 3 portions.
 * The first words that consist of capital letters.
 * The second words that begin with a capital letter.
 * The third is all lowercase letters. And count each element.
 * Translate the first and second elements of the array to lowercase, combine the resulting collections and group them by key
 */
@Slf4j
public class Challenge1 {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    String input = "gs://apache-beam-samples/shakespeare/kinglear.txt";
    PCollection<String> text = pipeline.apply(TextIO.read().from(input));
    PCollection<String> words = applyLinesToWords(text).apply(Filter.by(StringUtils::isNotEmpty));
    PCollectionList<String> partitions = applyPartitionWords(words);

//    Log samples for debugging
//    PTransform<PCollection<String>,PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);
//    partitions.get(0).apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new LogUtilTransforms.LogStrings("All Caps")));
//    partitions.get(1).apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new LogUtilTransforms.LogStrings("Starts Caps")));
//    partitions.get(2).apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new LogUtilTransforms.LogStrings("All lower")));

    PCollection<KV<String, Long>> allCapsCount = countPerElement(partitions.get(0));
    PCollection<KV<String, Long>> startsCapsCount = countPerElement(partitions.get(1));
    PCollection<KV<String, Long>> allLowerCount = countPerElement(partitions.get(2));

    PCollection<KV<String, Long>> allCapsToLowerCase = convertPCollectionToLowerCase(allCapsCount);
    PCollection<KV<String, Long>> allStartsWithCapsToLowerCase = convertPCollectionToLowerCase(startsCapsCount);

    PCollection<KV<String, Long>> merged = PCollectionList
      .of(allLowerCount)
      .and(allCapsToLowerCase)
      .and(allStartsWithCapsToLowerCase)
      .apply(Flatten.pCollections());

    PCollection<KV<String, Iterable<Long>>> finalOutput = merged.apply(GroupByKey.create());
    finalOutput.apply(ParDo.of(new LogUtilTransforms.LogOutput<>()));

    pipeline.run().waitUntilFinish();
  }

  static PCollection<KV<String, Long>> convertPCollectionToLowerCase(PCollection<KV<String, Long>> input) {
    return input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
      .via(word -> KV.of(word.getKey().toLowerCase(), word.getValue())));
  }

  static PCollection<KV<String,Long>> countPerElement(PCollection<String> input) {
    return input.apply(Count.perElement());
  }

  static PCollection<String> applyLinesToWords(PCollection<String> input) {
    return input.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(@Element String line, OutputReceiver<String> outputReceiver) {
        String[] words = line.split("[^\\p{L}]+");
        for (String word : words) {
          outputReceiver.output(word);
        }
      }
    }));
  }

  static PCollectionList<String> applyPartitionWords(PCollection<String> words) {
    return words
      .apply(Partition.of(3,
        new Partition.PartitionFn<String>() {

          @Override
          public int partitionFor(String elem, int numPartitions) {
            if (StringUtils.isAllUpperCase(elem)) {
              return 0;
            } else if (StringUtils.isAllLowerCase(elem)) {
              return 2;
            } else {
              return 1;
            }
          }
        }
      ));
  }
}
