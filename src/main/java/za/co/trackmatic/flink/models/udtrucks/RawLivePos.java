package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

/**
 * Represents a raw live position update of a vehicle including its metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawLivePos {

    private Vehicle vehicle;

    private Position position;

    private String triggerType;

    private String triggerTime;

    private String receivedTime;

    private TmMetadata tmMetadata;

    public RawLivePos() {

    }

    public Vehicle getVehicle() {
        return vehicle;
    }

    public void setVehicle(Vehicle vehicle) {
        this.vehicle = vehicle;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public String getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(String triggerType) {
        this.triggerType = triggerType;
    }

    public String getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(String triggerTime) {
        this.triggerTime = triggerTime;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    public TmMetadata getTmMetadata() {
        return tmMetadata;
    }

    public void setTmMetadata(TmMetadata tmMetadata) {
        this.tmMetadata = tmMetadata;
    }
}
