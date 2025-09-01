package za.co.trackmatic.flink.models.MultipleProviders;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;

/**
 * Represents an event where a set point value has changed.
 * <p>
 * The event contains information about the old and new set point values, the timestamp
 * of the change, the location where the event occurred, and a subtype specifying which
 * set point was changed.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SetPointEvent implements Serializable {

    /**
     * The event type identifier.
     */
    public static final String TYPE = "SET_POINT_CHANGED";

    /**
     * Subtype for when set point 1 has changed.
     */
    public static final String SUBTYPE_SETPOINT1_CHANGED = "setpoint1_changed";

    /**
     * Subtype for when set point 2 has changed.
     */
    public static final String SUBTYPE_SETPOINT2_CHANGED = "setpoint2_changed";

    /**
     * Subtype for when set point 3 has changed.
     */
    public static final String SUBTYPE_SETPOINT3_CHANGED = "setpoint3_changed";

    private String subtype;

    private double oldSetpoint;

    private double newSetpoint;

    private long timestamp;

    private LatLong location;

    /**
     * Default constructor.
     */
    public SetPointEvent() {

    }

    public double getOldSetpoint() {
        return oldSetpoint;
    }

    public void setOldSetpoint(double oldSetpoint) {
        this.oldSetpoint = oldSetpoint;
    }

    public double getNewSetpoint() {
        return newSetpoint;
    }

    public void setNewSetpoint(double newSetpoint) {
        this.newSetpoint = newSetpoint;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public LatLong getLocation() {
        return location;
    }

    public void setLocation(LatLong location) {
        this.location = location;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }
}
