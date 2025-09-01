package za.co.trackmatic.flink.models.MultipleProviders;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents an event indicating that the vehicle's fuel level is low.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LowFuelEvent implements Serializable {

    /**
     * Constant type identifier for low fuel events.
     */
    public static final String TYPE = "FUEL_LEVEL_LOW";

    /**
     * The type of the event, typically "FUEL_LEVEL_LOW".
     */
    private String type;

    /**
     * Timestamp of the event.
     */
    private long timestamp;

    /**
     * Current fuel level in the tank, typically as an integer percentage or volume.
     */
    private int fuelLevel;

    /**
     * Total size of the fuel tank.
     */
    private int fuelTankSize;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFuelLevel() {
        return fuelLevel;
    }

    public void setFuelLevel(int fuelLevel) {
        this.fuelLevel = fuelLevel;
    }

    public int getFuelTankSize() {
        return fuelTankSize;
    }

    public void setFuelTankSize(int fuelTankSize) {
        this.fuelTankSize = fuelTankSize;
    }

    public LowFuelEvent() {

    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
