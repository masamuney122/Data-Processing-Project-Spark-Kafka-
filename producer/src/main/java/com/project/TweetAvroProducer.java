package com.project;

import com.opencsv.CSVReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class TweetAvroProducer {

    public static void main(String[] args) throws Exception {

        // Kafka producer config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer =
                new KafkaProducer<>(props);

        // Load Avro schema
        InputStream schemaStream =
            TweetAvroProducer.class
                    .getClassLoader()
                    .getResourceAsStream("tweet.avsc");

        if (schemaStream == null) {
            throw new RuntimeException("tweet.avsc not found in resources");
        }

        Schema schema = new Schema.Parser().parse(schemaStream);

        // Read CSV
        InputStream csvStream =
        TweetAvroProducer.class
                .getClassLoader()
                .getResourceAsStream("Tweets.csv");

        if (csvStream == null) {
            throw new RuntimeException("Tweets.csv not found in resources");
        }

        CSVReader reader = new CSVReader(new InputStreamReader(csvStream));
        String[] line;
        reader.readNext(); // header skip

        while ((line = reader.readNext()) != null) {

            GenericRecord tweet = new GenericData.Record(schema);
            tweet.put("tweet_id", line[0]);
            tweet.put("airline_sentiment", line[1]);
            tweet.put("airline", line[6]);
            tweet.put("text", line[10]);
            tweet.put("tweet_created", line[11]);

            ProducerRecord<String, GenericRecord> record =
                    new ProducerRecord<>("tweets_topic", line[0], tweet);

            producer.send(record);
            Thread.sleep(100); // stream simülasyonu
        }

        producer.close();
        reader.close();
    }
}
