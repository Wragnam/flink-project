package za.co.trackmatic.flink.events;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents basic geofence data including its identifier and name.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeofenceData implements Serializable {

    /**
     * The unique identifier of the geofence.
     */
    private String id;

    /**
     * The name of the geofence.
     */
    private String name;

    public GeofenceData() {

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
