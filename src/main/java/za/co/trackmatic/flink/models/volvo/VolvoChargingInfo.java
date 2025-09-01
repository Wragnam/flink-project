package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents charging information related to a Volvo vehicle event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoChargingInfo implements Serializable {
    private String description;
    private String event;
    private String eventDetail;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getEventDetail() {
        return eventDetail;
    }

    public void setEventDetail(String eventDetail) {
        this.eventDetail = eventDetail;
    }
}
