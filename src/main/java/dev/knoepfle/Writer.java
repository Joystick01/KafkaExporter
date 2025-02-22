package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Writer {
    void write(ConsumerRecord<String, JsonNode> record) throws Exception;

    default void flush() throws Exception {// no-op
    }
}
