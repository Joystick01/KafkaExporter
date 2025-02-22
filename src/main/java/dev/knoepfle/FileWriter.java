package dev.knoepfle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileWriter implements Writer {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(FileWriter.class);

    ByteArrayOutputStream buffer;
    FileOutputStream fileOutputStream;

    private final Path path;
    private final int fileMessageCount;
    private final int padding;

    int currentFileMessage = 0;
    int currentFile = 0;

    public FileWriter(Path path, int fileMessageCount, int padding) {
        this.path = path;
        this.fileMessageCount = fileMessageCount;
        this.padding = padding;
        this.buffer = new ByteArrayOutputStream(620 * fileMessageCount);
    }

    @Override
    public void flush() throws Exception {
        logger.info("Writing file {}", currentFile);
        Files.createDirectories(path);
        File file = new File(path.resolve(String.format("%0" + padding + "d.json", currentFile)).toString());
        fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(buffer.toByteArray());
        fileOutputStream.close();
        buffer.reset();
        currentFileMessage = 0;
        currentFile++;
    }

    @Override
    public void write(ConsumerRecord<String, JsonNode> record) throws Exception {
        buffer.write(((ObjectNode) record.value()).put("kafkaTime", record.timestamp()).toString().getBytes());
        currentFileMessage++;
        if (currentFileMessage >= fileMessageCount) {
            flush();
        }
    }
}
