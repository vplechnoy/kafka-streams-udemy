package com.vplechnoy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

public class StreamsStarterApp
{
  public static void main(String[] args)
  {
    // 0 - create new streams builder instance
    StreamsBuilder builder = new StreamsBuilder();

    // 1 - stream from Kafka
    KStream<String, String> textLinesStream = builder.stream(
        "word-count-input", /* input topic */
        // this is how we explicitly specify Serdes for keys and values
        Consumed.with(
            Serdes.String(),  /* key serde */
            Serdes.String()   /* value serde */
        )
    );

    // 2 - map values to lower case
    KTable<String, Long> wordCountsTable = textLinesStream
        // 2 - map values to lowercase
        .mapValues((ValueMapper<String, String>) String::toLowerCase)
        // can be alternatively written as:
        // .mapValues(String::toLowerCase)
        // 3 - flatmap values split by space
        .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
        // 4 - select key to apply a key (we discard the old key)
        .selectKey((key, word) -> word)
        // 5 - group by key before aggregation
        .groupByKey()
        // 6 - count occurrences
        .count(Named.as("Counts"));

    // 7 - to in order to write the results back to kafka
    wordCountsTable
        .toStream()
        .map(KeyValue::new)
        .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    // Print topology to the console:
    System.out.println(topology.describe().toString());
  }
}
