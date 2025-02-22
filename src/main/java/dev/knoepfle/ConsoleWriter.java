package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsoleWriter implements Writer {

    String topic;

    public ConsoleWriter(String topic) {
        this.topic = topic;
    }

    @Override
    public void write(ConsumerRecord<String, JsonNode> record) {
        System.out.println("[" + topic + "] " + record.value().toString());
    }
}
