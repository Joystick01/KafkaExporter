package dev.knoepfle;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DataLakeWriter implements Writer {

    private static Logger logger = LoggerFactory.getLogger(DataLakeWriter.class);

    private final ByteArrayOutputStream buffer;
    private final DataLakeFileSystemClient dataLakeFileSystemClient;
    private DataLakeFileClient dataLakeFileClient;

    String subDir;

    int fileMessageCount;
    int currentFileMessage = 0;
    int currentFile = 0;
    int padding;

    public DataLakeWriter(
            String endpoint,
            String sasToken,
            String fileSystem,
            String subDir,
            int fileMessageCount,
            int padding
    ) {
        this.buffer = new ByteArrayOutputStream(620 * fileMessageCount);
        this.dataLakeFileSystemClient = new DataLakeFileSystemClientBuilder()
            .endpoint(endpoint)
            .sasToken(sasToken)
            .fileSystemName(fileSystem)
            .buildClient();

        this.subDir = subDir;

        this.fileMessageCount = fileMessageCount;
        this.padding = padding;

        logger.info("DataLakeWriter initialized");
    }

    @Override
    public void write(ConsumerRecord<String, JsonNode> record) throws IOException {
        buffer.write(record.value().toString().getBytes());
        currentFileMessage++;

        if (currentFileMessage == fileMessageCount) {
            logger.info("[ {} ] Uploading file {}", subDir, currentFile);
            dataLakeFileClient = dataLakeFileSystemClient.getFileClient(subDir + String.format("%0" + padding + "d.json", currentFile));
            dataLakeFileClient.upload(BinaryData.fromBytes(buffer.toByteArray()));
            buffer.reset();
            currentFileMessage = 0;
            currentFile++;
        }

    }

    @Override
    public void flush() throws IOException {
        if (currentFileMessage > 0) {
            logger.info("[ {} ] Uploading file {}", subDir, currentFile);
            dataLakeFileClient = dataLakeFileSystemClient.getFileClient(subDir + String.format("%0" + padding + "d.json", currentFile));
            dataLakeFileClient.upload(BinaryData.fromBytes(buffer.toByteArray()));
            buffer.reset();
            currentFileMessage = 0;
            currentFile++;
        }
    }

}
