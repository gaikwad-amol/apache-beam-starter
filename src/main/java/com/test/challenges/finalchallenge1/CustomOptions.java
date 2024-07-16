package com.test.challenges.finalchallenge1;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions {

  @Description("Path of the file to read from")
  @Default.String("src/main/resources/FinalChallenge1Input.csv")
  String getInputFile();

  void setInputFile(String value);

  @Description("Path of the file price_more_than_10.txt to write to")
  @Default.String("price_more_than_10.txt")
//    @Validation.Required
  String getPriceMoreThan10();

  void setPriceMoreThan10(String value);

  @Description("Path of the file price_less_than_10.txt to write to")
  @Default.String("price_less_than_10.txt")
//  @Validation.Required
  String getPriceLessThan10();

  void setPriceLessThan10(String value);
}
