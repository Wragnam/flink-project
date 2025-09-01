package za.co.trackmatic.flink.events.mix;

import java.io.Serializable;

/**
 * Represents specific event data from the Mix system.
 */
public class MixSpecificData implements Serializable {
    /**
     * The unique identifier of the Mix event.
     */
    private String eventId;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}
