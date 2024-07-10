package com.test;

import com.test.transforms.LogUtilTransforms;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

@Slf4j
public class MultiOutputReceiver {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers = pipeline.apply("Numbers", Create.of(10, 50, 120, 20, 200, 0));

    TupleTag<Integer> numbersBelow100 = new TupleTag<>() {
    };
    TupleTag<Integer> numbersAbove100 = new TupleTag<>() {
    };

    PCollectionTuple pCollectionTuple = applyTransform(numbers, numbersBelow100, numbersAbove100);
    pCollectionTuple.get(numbersBelow100).apply("LogNumbers < 100", ParDo.of(new LogUtilTransforms.LogIntegers("Number < 100")));
    pCollectionTuple.get(numbersAbove100).apply("LogNumbers > 100", ParDo.of(new LogUtilTransforms.LogIntegers("Number > 100")));


    pipeline.run().waitUntilFinish();

  }

  static PCollectionTuple applyTransform(PCollection<Integer> input,
                                         TupleTag<Integer> numBelow100Tag,
                                         TupleTag<Integer> numAbove100Tag) {

    return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

      @ProcessElement
      public void processElement(@Element Integer number, MultiOutputReceiver out) {
        if (number <= 100) {
          // First PCollection
          out.get(numBelow100Tag).output(number);
        } else {
          // Additional PCollection
          out.get(numAbove100Tag).output(number);
        }
      }

    }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
  }
}
