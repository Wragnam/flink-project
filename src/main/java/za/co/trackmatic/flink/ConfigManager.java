package za.co.trackmatic.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import za.co.trackmatic.flink.models.Config;

import java.io.File;

/**
 * Manages loading and accessing configuration from a JSON file.
 * <p>
 * The configuration is expected to be in a file named "config.json" inside the specified base directory.
 * The configuration must contain valid Kafka source and sink settings.
 */
public class ConfigManager {

    private final String baseDir;

    private transient Config config;

    /**
     * Constructs a ConfigManager and reads the configuration file from the specified base directory.
     *
     * @param baseDir The base directory containing "config.json".
     * @throws Exception if the configuration file is malformed or cannot be read.
     */
    public ConfigManager(String baseDir) throws Exception {
        this.baseDir = baseDir;
        readConfigFile();
    }

    /**
     * Reads the "config.json" file from the base directory and parses it into a Config object.
     * Validates that Kafka source and sink configurations exist.
     *
     * @throws Exception if the file is missing, malformed, or missing required Kafka configurations.
     */
    private void readConfigFile() throws Exception {
        ObjectMapper om = new ObjectMapper();
        config = om.readValue(new File(this.baseDir + "/config.json"), Config.class);
        if (config == null ||
                config.getKafka() == null ||
                config.getKafka().getSource() == null ||
                config.getKafka().getSink() == null) {
            throw new Exception("Malformed config file");
        }
    }

    public String getBaseDir() {
        return baseDir;
    }

    public Config getConfig() {
        return config;
    }
}
