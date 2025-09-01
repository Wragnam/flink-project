package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents metadata about a specific location, typically associated with an event or geofence.
 * <p>
 * Contains a location ID and a descriptive name.
 * </p>
 */
public class LocationInfo implements Serializable {

    /**
     * Unique identifier for the location.
     */
    private String id;

    /**
     * Descriptive name of the location.
     */
    private String name;

    public LocationInfo() {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
