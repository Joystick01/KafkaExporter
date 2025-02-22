package dev.knoepfle;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ConfigurationManager {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(ConfigurationManager.class);

    private static ConfigurationManager instance;
    private final Map<String, Object> configuration = new HashMap<>();

    private final String[] parameters = {
            "WRITER_TYPE",
            "DATALAKE_ENDPOINT",
            "DATALAKE_SAS_TOKEN",
            "DATALAKE_FILESYSTEM",
            "DATALAKE_FILE_MESSAGE_COUNT",
            "DATALAKE_PADDING",
            "DATALAKE_PREFIX",
            "FILEWRITER_PATH",
            "FILEWRITER_FILE_MESSAGE_COUNT",
            "FILEWRITER_PADDING",
            "KAFKA_TOPICS",
            "BOOTSTRAP_SERVERS_CONFIG",
            "MAX_POLL_RECORDS_CONFIG"
    };

    private ConfigurationManager() {
        for (String parameter : parameters) {
            configuration.put(parameter, System.getenv(parameter));
        }
        configuration.putIfAbsent("DATALAKE_PREFIX", "");

        logger.info("Configuration loaded: {}", configuration);
    }

    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    public String getString(String key) {
        if (configuration.get(key) == null) {
            throw new IllegalArgumentException("Configuration key not found: " + key);
        }
        return (String) configuration.get(key);
    }

    public String[] getStringArray(String key) {
        if (configuration.get(key) == null) {
            throw new IllegalArgumentException("Configuration key not found: " + key);
        }
        return ((String) configuration.get(key)).split(",");
    }

    public int getInt(String key) {
        if (configuration.get(key) == null) {
            throw new IllegalArgumentException("Configuration key not found: " + key);
        }
        return Integer.parseInt((String) configuration.get(key));
    }
}