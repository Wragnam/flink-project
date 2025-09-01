package za.co.trackmatic.flink.models.MultipleProviders;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;

/**
 * Represents a door state event indicating whether a specific door or zone door
 * has been opened or closed at a particular timestamp and location.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DoorStateEvent implements Serializable {

    /** Event type constant */
    public static final String TYPE = "DOOR_STATE";

    /** Subtype constants representing various door open/closed states */
    public static final String SUBTYPE_ZONE1_DOOR_OPEN = "zone1_door_open";
    public static final String SUBTYPE_ZONE1_DOOR_CLOSED = "zone1_door_closed";
    public static final String SUBTYPE_ZONE2_DOOR_OPEN = "zone2_door_open";
    public static final String SUBTYPE_ZONE2_DOOR_CLOSED = "zone2_door_closed";
    public static final String SUBTYPE_ZONE3_DOOR_OPEN = "zone3_door_open";
    public static final String SUBTYPE_ZONE3_DOOR_CLOSED = "zone3_door_closed";
    public static final String SUBTYPE_REAR_DOOR_OPEN = "rear_door_open";
    public static final String SUBTYPE_REAR_DOOR_CLOSED = "rear_door_closed";
    public static final String SUBTYPE_SIDE_DOOR_OPEN = "side_door_open";
    public static final String SUBTYPE_SIDE_DOOR_CLOSED = "side_door_closed";

    /** The subtype indicating which door and state the event refers to */
    private String subtype;

    /** The event timestamp */
    private long timestamp;

    /** The geographical location (latitude and longitude) where the event occurred */
    private LatLong location;

    public DoorStateEvent() {

    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
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
}
