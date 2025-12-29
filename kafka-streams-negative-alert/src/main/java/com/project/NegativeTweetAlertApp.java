package com.project;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class NegativeTweetAlertApp {

    public static class ConsoleExceptionHandler implements DeserializationExceptionHandler {
        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
            System.err.println(">>> HATA TESPIT EDILDI (Veri Atlaniyor): " + exception.getMessage());
            return DeserializationHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        // ID'yi v5 yaptım ki offset sıfırlansın, temiz temiz sadece dolu olanları bassın
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "negative-tweet-alert-app-final");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ConsoleExceptionHandler.class);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");

        GenericAvroSerde valueSerde = new GenericAvroSerde();
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", "http://localhost:8081");
        valueSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> stream = builder.stream(
                "tweets_topic",
                Consumed.with(Serdes.String(), valueSerde)
        );

        stream
            .filter((key, record) -> {
                if (record == null) return false;
                
                // 1. Sentiment Kontrolü: NEGATIVE mi?
                Object sentimentObj = record.get("airline_sentiment");
                boolean isNegative = sentimentObj != null && "negative".equalsIgnoreCase(sentimentObj.toString());
                
                // 2. Havayolu İsmi Kontrolü: DOLU mu?
                Object airlineObj = record.get("airline");
                boolean hasAirlineName = airlineObj != null && !airlineObj.toString().trim().isEmpty();

                // İkisi de doğruysa geçir
                return isNegative && hasAirlineName;
            })
            .foreach((key, record) -> {
                // Artık buraya sadece ismi olanlar gelecek
                String airline = record.get("airline").toString();
                String text = record.get("text") != null ? record.get("text").toString() : "";
                
                System.out.println("ALERT [" + airline + "]: " + text);
            });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println(">>> Kafka Streams (v5 - Filtreli) Baslatildi...");
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}