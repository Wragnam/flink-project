package za.co.trackmatic.flink.events.lytx;

import java.io.Serializable;

/**
 * Represents specific event data from the Lytx system.
 */
public class LytxSpecificEventData implements Serializable {

    /**
     * The unique identifier of the Lytx event.
     */
    private String eventId;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}
