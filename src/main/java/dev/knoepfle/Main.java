package dev.knoepfle;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        ConfigurationManager configurationManager = ConfigurationManager.getInstance();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationManager.getString("BOOTSTRAP_SERVERS_CONFIG"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configurationManager.getInt("MAX_POLL_RECORDS_CONFIG"));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<String> topics = Arrays.asList(configurationManager.getStringArray("KAFKA_TOPICS"));
        Map<String, Writer> writers = new HashMap<>();

        consumer.subscribe(topics);
        ConsumerRecords<String, String> records;

        for (String topic : topics) {

            if (configurationManager.getString("WRITER_TYPE").equals("DATALAKE")) {
                writers.put(topic, new DataLakeWriter(
                    configurationManager.getString("DATALAKE_ENDPOINT"),
                    configurationManager.getString("DATALAKE_SAS_TOKEN"),
                    configurationManager.getString("DATALAKE_FILESYSTEM"),
                    configurationManager.getString("DATALAKE_PREFIX") + "/" + topic,
                    configurationManager.getInt("DATALAKE_FILE_MESSAGE_COUNT"),
                    configurationManager.getInt("DATALAKE_PADDING")
                ));
            } else if (configurationManager.getString("WRITER_TYPE").equals("CONSOLE")) {
                writers.put(topic, new ConsoleWriter(topic));
            }
            else {
                throw new IllegalArgumentException("Invalid writer type");
            }
        }

        do {
            records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                try {
                    writers.get(record.topic()).write(record.value());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } while (!records.isEmpty());
    }

}