package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents an event related to a geofence, such as an arrival or departure.
 * Contains information about the geofence, the event timestamp, and location.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeofenceEvent implements Serializable {

    /** Constant type identifier for geofence events */
    public static final String TYPE = "GEOFENCE";

    /** Constant subtype identifier for departure events */
    public static final String SUBTYPE_DEPARTURE = "DEPARTURE";

    /** Constant subtype identifier for arrival events */
    public static final String SUBTYPE_ARRIVAL = "ARRIVAL";

    /** The type of the event (e.g., "GEOFENCE") */
    private String type;

    /** The timestamp of when the event occurred */
    private long timestamp;

    /** Identifier of the geofence related to this event */
    private String geofenceId;

    /** The geographical location where the event occurred */
    private LatLong location;

    /** The name of the geofence */
    private String geofenceName;

    /** Flag indicating if the event was forced (not detected naturally) */
    private boolean forced = false;

    public GeofenceEvent() {

    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getGeofenceId() {
        return geofenceId;
    }

    public void setGeofenceId(String geofenceId) {
        this.geofenceId = geofenceId;
    }

    public LatLong getLocation() {
        return location;
    }

    public void setLocation(LatLong location) {
        this.location = location;
    }

    public boolean isForced() {
        return forced;
    }

    public void setForced(boolean forced) {
        this.forced = forced;
    }

    public String getGeofenceName() {
        return geofenceName;
    }

    public void setGeofenceName(String geofenceName) {
        this.geofenceName = geofenceName;
    }
}
