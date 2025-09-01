package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a state with a name, timestamp, and value.
 * This class is typically used to capture the state information
 * related to a fleetboard position or event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class State implements Serializable {
    private String name;

    private String timestamp;

    private String value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
