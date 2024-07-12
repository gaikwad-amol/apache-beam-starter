package com.test;

import com.test.transforms.LogUtilTransforms;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@Slf4j
public class SelectSchemaAndOutputFields {

  @DefaultSchema(JavaFieldSchema.class)
  public static class Game {
    public String userId;
    public String score;
    public String gameId;
    public String date;

    @SchemaCreate
    public Game(String userId, String score, String gameId, String date) {
      this.userId = userId;
      this.score = score;
      this.gameId = gameId;
      this.date = date;
    }

    @Override
    public String toString() {
      return "Game{" +
        "userId='" + userId + '\'' +
        ", score='" + score + '\'' +
        ", gameId='" + gameId + '\'' +
        ", date='" + date + '\'' +
        '}';
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class User {
    public String userId;
    public String userName;

    public Game game;

    @SchemaCreate
    public User(String userId, String userName, Game game) {
      this.userId = userId;
      this.userName = userName;
      this.game = game;
    }

    @Override
    public String toString() {
      return "User{" +
        "userId='" + userId + '\'' +
        ", userName='" + userName + '\'' +
        ", game=" + game +
        '}';
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    Schema shortInfoSchema = Schema.builder()
      .addStringField("userId")
      .addStringField("userName")
      .build();

    Schema gameSchema = Schema.builder()
      .addStringField("userId")
      .addStringField("score")
      .addStringField("gameId")
      .addStringField("date")
      .build();

    Schema dataSchema = Schema.builder()
      .addStringField("userId")
      .addStringField("userName")
      .addRowField("game", gameSchema)
      .build();

    PCollection<String> gamingData = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));

    final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);
    PCollection<String> sampleData = gamingData.apply(sample).apply(Flatten.iterables());

    PCollection<User> users = sampleData.apply(ParDo.of(new ExtractUserProgressFn()))
      .setSchema(dataSchema, TypeDescriptor.of(User.class), row -> {
        User user = row;
        Game game = user.game;

        Row gameRow = Row.withSchema(gameSchema)
          .addValues(game.userId, game.score, game.gameId, game.date).build();

        return Row.withSchema(dataSchema).addValues(user.userId, user.userName, gameRow).build();
      }, row -> {
        String userId = row.getValue("userId");
        String userName = row.getValue("userName");
        Row game = row.getValue("game");

        String gameId = game.getValue("gameId");
        String gameScore = game.getValue("score");
        String gameDate = game.getValue("date");
        return new User(userId,userName,
          new Game(userId,gameScore,gameId,gameDate));
        }
      );
    PCollection<Row> shortInfoSchemaCollection = users.apply(Select.<User>fieldNames("userId", "userName").withOutputSchema(shortInfoSchema));
    shortInfoSchemaCollection.apply("Short Info", ParDo.of(new LogUtilTransforms.LogOutput<>()));

    PCollection<Row> gameSchemaCollection = users.apply(Select.<User>fieldNames("game.*").withOutputSchema(gameSchema));
    gameSchemaCollection.apply("Game", ParDo.of(new LogUtilTransforms.LogOutput<>()));

    PCollection<Row> flattened = users
      .apply(Select.flattenedSchema())
      .apply("User flatten row", ParDo.of(new LogUtilTransforms.LogOutput<>()));

    pipeline.run().waitUntilFinish();
  }


  static class ExtractUserProgressFn extends DoFn<String, User> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<User> out) {
      String[] items = row.split(",");
      out.output(new User(items[0], items[1], new Game(items[0], items[2], items[3], items[4])));
    }
  }

}
