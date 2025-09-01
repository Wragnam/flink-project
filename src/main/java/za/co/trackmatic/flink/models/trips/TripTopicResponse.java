package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents a response message sent on a trip-related topic,
 * containing metadata about the message and a payload with the actual data.
 */
public class TripTopicResponse implements Serializable {

    /** Metadata associated with the response message. */
    private Metadata meta;

    /** Payload data of the response. Can be any type of object. */
    private Object payload;

    public TripTopicResponse(){
        this.meta = new Metadata();
    }

    public Metadata getMeta() {
        return meta;
    }

    public void setMeta(Metadata meta) {
        this.meta = meta;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
