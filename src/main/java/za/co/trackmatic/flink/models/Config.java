package za.co.trackmatic.flink.models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration class representing the application configuration including Kafka settings.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Config implements Serializable {

    /**
     * Inner class representing Kafka source or sink configuration.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KafkaSourceSink implements Serializable {

        @JsonProperty("bootstrap_servers")
        private String bootstrapServers;

        private String topics;

        /**
         * The group id for kafka. Required for consumers (sources). Can be null
         * for producers (sinks)
         */
        @JsonProperty("group_id")
        private String groupId;

        private List<KeyValuePair> properties;

        public KafkaSourceSink() {

        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopics() {
            return topics;
        }

        public void setTopics(String topics) {
            this.topics = topics;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<KeyValuePair> getProperties() {
            return properties;
        }

        public void setProperties(List<KeyValuePair> properties) {
            this.properties = properties;
        }
    }

    /**
     * Inner class representing Kafka configuration with separate source and sink.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Kafka implements Serializable {
        private KafkaSourceSink source;
        private KafkaSourceSink sink;

        public Kafka() {

        }

        public KafkaSourceSink getSource() {
            return source;
        }

        public void setSource(KafkaSourceSink source) {
            this.source = source;
        }

        public KafkaSourceSink getSink() {
            return sink;
        }

        public void setSink(KafkaSourceSink sink) {
            this.sink = sink;
        }
    }

    @JsonProperty("processor_type")
    private String processorType;

    @JsonProperty("datacache_server")
    private String datacacheServer;

    @JsonProperty("flink_job_name")
    private String flinkJobName;

    private Kafka kafka;

    public Config() {

    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    public String getDatacacheServer() {
        return datacacheServer;
    }

    public void setDatacacheServer(String datacacheServer) {
        this.datacacheServer = datacacheServer;
    }

    public String getFlinkJobName() {
        return flinkJobName;
    }

    public void setFlinkJobName(String flinkJobName) {
        this.flinkJobName = flinkJobName;
    }
}
