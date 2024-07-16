package com.test.challenges.finalchallenge1;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.apache.beam.sdk.extensions.sql.SqlTransform;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * You’re given a csv file with purchase transactions. Write a Beam pipeline to prepare a report every 30 seconds.
 * The report needs to be created only for transactions where quantity is more than 20.
 * Report should consist of two files named "price_more_than_10.txt" and "price_less_than_10.txt":
 * •Total transactions amount grouped by ProductNo for products with price greater than 10
 * •Total transactions amount grouped by ProductNo for products with price less than 10
 */
@Slf4j
public class FinalChallenge1 {

  private static final Schema OUTPUT_SCHEMA = Schema.builder()
    .addStringField("ProductNo")
    .addDoubleField("TotalAmount")
    .build();
  private static final String HEADER = "ProductNo,TotalAmount";
  private static final String REGEX_FOR_CSV = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final long TIME_OUTPUT_AFTER_FIRST_ELEMENT = 5;
  public static final int WINDOW_TIME = 3;

  public static void main(String[] args) {
    CustomOptions customOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);

    Pipeline pipeline = Pipeline.create(customOptions);
    PCollection<String> input = pipeline.apply("Read", TextIO.read().from(customOptions.getInputFile()));

    //TransactionNo,Date,ProductNo,ProductName,Price,Quantity,CustomerNo,Country
    Schema schema = createSchema();

    PCollection<Row> oldtableRows = input
      .apply(MapElements.into(TypeDescriptors.rows())
        .via(extractRow(schema)))
      .setRowSchema(schema)
      .apply(Filter.by(Objects::nonNull));
    PCollection<Row> tableRows = applyTimeStamp(oldtableRows).setRowSchema(schema);

    Window<Row> window = Window.into(FixedWindows.of(Duration.standardHours(WINDOW_TIME)));
    Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(TIME_OUTPUT_AFTER_FIRST_ELEMENT));

    PCollection<Row> windowedRows = tableRows
      .apply(window.triggering(Repeatedly.forever(trigger)).withAllowedLateness(Duration.ZERO).discardingFiredPanes())
      .apply(Filter.by(row -> row.getInt32("Quantity") > 20));

    PCollection<Row> priceMoreThan10Result = windowedRows.apply(
      SqlTransform.query(
        "SELECT ProductNo, sum(Price) as TotalAmount from PCOLLECTION where Price > 10 GROUP BY ProductNo"
      )).setRowSchema(OUTPUT_SCHEMA);

    PCollection<String> priceMoreThan10ResultAsString = priceMoreThan10Result.apply(
      MapElements.into(TypeDescriptors.strings())
        .via((Row row) -> row.getString("ProductNo") + ", " + row.getDouble("TotalAmount"))
    );
    priceMoreThan10ResultAsString.apply(TextIO.write().withoutSharding().to("price_more_than_10").withSuffix(".txt").withHeader(HEADER));

    PCollection<Row> priceLess10Result = windowedRows.apply(
      SqlTransform.query(
        "SELECT ProductNo, sum(Price) as TotalAmount from PCOLLECTION where Price < 10 GROUP BY ProductNo"
      )).setRowSchema(OUTPUT_SCHEMA);

    PCollection<String> priceLess10ResultResultAsString = priceLess10Result.apply(
      MapElements.into(TypeDescriptors.strings())
        .via((Row row) -> row.getString("ProductNo") + ", " + row.getDouble("TotalAmount"))
    );
    priceLess10ResultResultAsString.apply(TextIO.write().withoutSharding().to("price_less_than_10").withSuffix(".txt").withHeader(HEADER));

    pipeline.run().waitUntilFinish();

  }

  static PCollection<Row> applyTimeStamp(PCollection<Row> input) {
    return input.apply(ParDo.of(new DoFn<Row, Row>() {

      @ProcessElement
      public void processElement(@Element Row row, OutputReceiver<Row> out) {
        Instant instant = row.getValue("Date");
        out.outputWithTimestamp(row, instant);
      }
    }));
    }

  private static @NotNull Schema createSchema() {
    return Schema.builder()
      .addInt32Field("TransactionNo")
      .addDateTimeField("Date")
      .addStringField("ProductNo")
      .addStringField("ProductName")
      .addDoubleField("Price")
      .addInt32Field("Quantity")
      .addInt32Field("CustomerNo")
      .addStringField("Country")
      .build();
  }

  private static @NotNull SerializableFunction<String, @UnknownKeyFor @NonNull @Initialized Row> extractRow(Schema schema) {
    return line -> {
      try {
        String[] values = line.split(REGEX_FOR_CSV);

        LocalDateTime dateTime = LocalDateTime.parse(values[1], FORMATTER);
        java.time.Instant javaInstant = dateTime.toInstant(ZoneOffset.UTC);
        Instant jodaInstant = new Instant(javaInstant.toEpochMilli());

        int transactionNo = Integer.parseInt(values[0]);
        double price = Double.parseDouble(values[4]);
        int qty = Integer.parseInt(values[5]);
        int customerNo = Integer.parseInt(values[6]);
        return Row.withSchema(schema)
          .withFieldValue("TransactionNo", transactionNo)
          .withFieldValue("Date", jodaInstant)
          .withFieldValue("ProductNo", values[2])
          .withFieldValue("ProductName", values[3])
          .withFieldValue("Price", price)
          .withFieldValue("Quantity", qty)
          .withFieldValue("CustomerNo", customerNo)
          .withFieldValue("Country", values[7])
          .build();
      } catch (Throwable e) {
        log.error("Exception while parsing: ", e);
      }
      return null;

    };
  }
}
