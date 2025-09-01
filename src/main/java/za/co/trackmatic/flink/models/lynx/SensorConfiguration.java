package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the configuration details of a sensor installed on an asset.
 * This includes sensor type, configuration status, location, connection point, and any relevant sensor data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorConfiguration implements Serializable {
    private String sensorType;
    private Boolean configured;
    private String sensorLocation;
    private String connectionLocation;
    private String sensorData;

    public String getSensorType() {
        return sensorType;
    }

    public void setSensorType(String sensorType) {
        this.sensorType = sensorType;
    }

    public Boolean getConfigured() {
        return configured;
    }

    public void setConfigured(Boolean configured) {
        this.configured = configured;
    }

    public String getSensorLocation() {
        return sensorLocation;
    }

    public void setSensorLocation(String sensorLocation) {
        this.sensorLocation = sensorLocation;
    }

    public String getConnectionLocation() {
        return connectionLocation;
    }

    public void setConnectionLocation(String connectionLocation) {
        this.connectionLocation = connectionLocation;
    }

    public String getSensorData() {
        return sensorData;
    }

    public void setSensorData(String sensorData) {
        this.sensorData = sensorData;
    }
}
