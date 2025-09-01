package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw live position report for a Volvo vehicle.
 * Contains vehicle identification, trigger information, timestamps,
 * GPS position, speed data, and associated metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoLiveposRaw implements Serializable {
    private String vin;

    private VolvoTrigger triggerType;

    private String createdDateTime;

    private String receivedDateTime;

    private VolvoGnssPosition gnssPosition;

    private double wheelBasedSpeed;

    private double tachographSpeed;

    private TmMetadata metadata;

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public VolvoTrigger getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(VolvoTrigger triggerType) {
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

    public VolvoGnssPosition getGnssPosition() {
        return gnssPosition;
    }

    public void setGnssPosition(VolvoGnssPosition gnssPosition) {
        this.gnssPosition = gnssPosition;
    }

    public double getWheelBasedSpeed() {
        return wheelBasedSpeed;
    }

    public void setWheelBasedSpeed(double wheelBasedSpeed) {
        this.wheelBasedSpeed = wheelBasedSpeed;
    }

    public double getTachographSpeed() {
        return tachographSpeed;
    }

    public void setTachographSpeed(double tachographSpeed) {
        this.tachographSpeed = tachographSpeed;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
