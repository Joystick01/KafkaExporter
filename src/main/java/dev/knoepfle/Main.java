package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

public class Main {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        logger.info("Starting Kafka export");

        ConfigurationManager configurationManager = ConfigurationManager.getInstance();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationManager.getString("BOOTSTRAP_SERVERS_CONFIG"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configurationManager.getInt("MAX_POLL_RECORDS_CONFIG"));

        KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props);

        List<String> topics = Arrays.asList(configurationManager.getStringArray("KAFKA_TOPICS"));
        Map<String, Writer> writers = new HashMap<>();

        consumer.subscribe(topics);
        ConsumerRecords<String, JsonNode> records;

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
            } else if (configurationManager.getString("WRITER_TYPE").equals("FILE")) {
                writers.put(topic, new FileWriter(
                    Paths.get(configurationManager.getString("FILEWRITER_PATH") + "/" + topic),
                    configurationManager.getInt("FILEWRITER_FILE_MESSAGE_COUNT"),
                    configurationManager.getInt("FILEWRITER_PADDING")
                ));
            } else {
                throw new IllegalArgumentException("Invalid writer type");
            }
        }

        logger.info("Starting to consume messages from Kafka topics: {}", topics);

        do {
            records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                try {
                    writers.get(record.topic()).write(record);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } while (!records.isEmpty());

        logger.info("Finished consuming messages from Kafka topics: {}", topics);

        writers.values().forEach(
            writer -> {
                try {
                    writer.flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );

        logger.info("Finished Kafka export");
    }

}