package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a message containing information about a trip and its associated events.
 * This includes context about the trip, a list of event snapshots, and the most recent status.
 */
public class TripEventMessage implements Serializable {

    /**
     * Constructs a new {@code TripEventMessage} with a unique ID and an empty list of event snapshots.
     */
    public TripEventMessage(){
        this.eventList = new ArrayList<>();
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Contextual information about the trip such as asset, operator, and site details.
     */
    private Context context;

    /**
     * A unique identifier for the trip event message.
     */
    private String id;

    /**
     * The list of event snapshots associated with the trip.
     */
    private List<TripEventSnapshot> eventList;

    /**
     * The last known status of the trip (e.g., "STARTED", "COMPLETED").
     */
    private String lastStatus;

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<TripEventSnapshot> getEventList() {
        return eventList;
    }

    public void setEventList(List<TripEventSnapshot> eventList) {
        this.eventList = eventList;
    }

    public String getLastStatus() {
        return lastStatus;
    }

    public void setLastStatus(String lastStatus) {
        this.lastStatus = lastStatus;
    }
}
