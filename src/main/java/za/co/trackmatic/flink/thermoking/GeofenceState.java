package za.co.trackmatic.flink.thermoking;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the state of a geofence for a specific entity (e.g., vehicle or asset).
 * This class is used to track whether an entity is currently inside or outside a geofence,
 * along with associated metadata like geofence ID, name, and timestamp.
 *
 * <p>Jackson will ignore any unknown properties during deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeofenceState implements Serializable {

    /**
     * Enum representing the possible states relative to a geofence:
     * <ul>
     *     <li>{@code UNKNOWN} - The state is not yet determined.</li>
     *     <li>{@code IN} - The entity is inside the geofence.</li>
     *     <li>{@code OUT} - The entity is outside the geofence.</li>
     * </ul>
     */
    public enum States {
        UNKNOWN,
        IN,
        OUT
    }

    private States state = States.UNKNOWN;
    private String name;
    private String id;
    private String date;

    public GeofenceState() {

    }

    public States getState() {
        return state;
    }

    public void setState(States state) {
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
