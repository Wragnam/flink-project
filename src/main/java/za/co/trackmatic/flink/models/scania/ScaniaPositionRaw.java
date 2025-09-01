package za.co.trackmatic.flink.models.scania;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw position message from a Scania vehicle.
 * This class includes vehicle VIN, GNSS position, metadata, and timestamps for when data was created and received.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScaniaPositionRaw implements Serializable {
    private String vin;

    private TriggerType triggerType;

    private String createdDateTime;

    private String receivedDateTime;

    private GNSSPosition gnssPosition;

    private double wheelBasedSpeed;

    private TmMetadata metadata;

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public TriggerType getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(TriggerType triggerType) {
        this.triggerType = triggerType;
    }

    public String getCreatedDateTime() {
        return createdDateTime;
    }

    public void setCreatedDateTime(String createdDateTime) {
        this.createdDateTime = createdDateTime;
    }

    public String getReceivedDateTime() {
        return receivedDateTime;
    }

    public void setReceivedDateTime(String receivedDateTime) {
        this.receivedDateTime = receivedDateTime;
    }

    public GNSSPosition getGnssPosition() {
        return gnssPosition;
    }

    public void setGnssPosition(GNSSPosition gnssPosition) {
        this.gnssPosition = gnssPosition;
    }

    public double getWheelBasedSpeed() {
        return wheelBasedSpeed;
    }

    public void setWheelBasedSpeed(double wheelBasedSpeed) {
        this.wheelBasedSpeed = wheelBasedSpeed;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
