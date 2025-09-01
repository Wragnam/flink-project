package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents a generic event that may occur during a trip.
 * <p>
 * This can be used to describe operational or logistical events
 * such as loading, unloading, arrival, departure, etc.
 * </p>
 */
public class Event implements Serializable {

    public Event(){}

    /**
     * The unique identifier of the event.
     */
    private String id;

    /**
     * The name of the event (e.g., "SPEEDING").
     */
    private String name;

    /**
     * The type of the event.
     */
    private String type;

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
