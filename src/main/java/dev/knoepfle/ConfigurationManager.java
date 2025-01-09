package dev.knoepfle;

import java.util.HashMap;
import java.util.Map;

public class ConfigurationManager {

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
            "KAFKA_TOPICS",
            "KAFKA_BOOTSTRAP_SERVERS",
            "MAX_POLL_RECORDS_CONFIG"
    };

    private ConfigurationManager() {
        for (String parameter : parameters) {
            configuration.put(parameter, System.getenv(parameter));
        }
        configuration.putIfAbsent("DATALAKE_PREFIX", "");
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