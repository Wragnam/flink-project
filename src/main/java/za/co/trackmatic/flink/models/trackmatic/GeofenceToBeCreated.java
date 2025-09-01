package za.co.trackmatic.flink.models.trackmatic;

import java.io.Serializable;

/**
 * Represents a geofence that is to be created,
 * containing its type, identifier, and name.
 */
public class GeofenceToBeCreated implements Serializable {

    public GeofenceToBeCreated(){}

    /**
     * The type of the geofence (e.g., ARRIVAL, DEPARTURE).
     */
    private String type;

    /**
     * The unique identifier of the geofence.
     */
    private String id;

    /**
     * The name of the geofence.
     */
    private String name;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
