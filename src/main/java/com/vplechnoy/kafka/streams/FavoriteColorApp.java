package com.vplechnoy.kafka.streams;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp
{
  public static void main(String[] args)
  {
    // 0 - create new streams builder instance
    StreamsBuilder builder = new StreamsBuilder();

    // 1 - stream from Kafka
    KStream<String, String> textLinesStream = builder.stream(
        "favorite-color-input", /* input topic */
        // this is how we explicitly specify Serdes for keys and values
        Consumed.with(
            Serdes.String(),  /* key serde */
            Serdes.String()   /* value serde */
        )
    );

    // Step 1: create the topic of users as keys and colors as values
    KStream<String, String> usersAndColorsStream = textLinesStream
        // 2 - filter bad values
        .filter((key, value) -> StringUtils.isNotEmpty(value) && value.split(",").length == 2)
        // 3 - selectKey that will be the user id
        .selectKey((key, value) -> value.split(",")[0].toLowerCase())
        // 4 - extract color as lowercase
        .mapValues((key, value) -> value.split(",")[1].toLowerCase())
        // 5 - remove bad colors
        .filter((user, color) -> Arrays.asList("red", "green", "blue").contains(color));

    usersAndColorsStream.to("user-keys-and-colors");

    // Step 2: read the topic of users as keys and colors as values as a KTable so that updates are read correctly
    KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colors");

    // Step 3: we count the occurrences of colors
    KTable<String, Long> favoriteColors = usersAndColorsTable.groupBy(
        (user, color) -> new KeyValue<>(color, color)
    )
    .count(Named.as("CountsByColors"));

    // Step 4: output the results back to Kafka
    favoriteColors
        .toStream()
        .map(KeyValue::new)
        .to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.cleanUp(); // do this in dev but not in production
    streams.start();

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    // Print topology to the console:
    System.out.println(topology.describe().toString());
  }
}
