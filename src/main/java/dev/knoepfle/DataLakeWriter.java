package dev.knoepfle;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DataLakeWriter implements Writer {

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
    }

    @Override
    public void write(String message) throws IOException {
        buffer.write(message.getBytes());
        currentFileMessage++;

        if (currentFileMessage == fileMessageCount) {
            System.out.println("Uploading file " + currentFile);
            dataLakeFileClient = dataLakeFileSystemClient.getFileClient(subDir + String.format("%0" + padding + "d.json", currentFile));
            dataLakeFileClient.upload(BinaryData.fromBytes(buffer.toByteArray()));
            buffer.reset();
            currentFileMessage = 0;
            currentFile++;
        }

    }

}
