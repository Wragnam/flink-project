package za.co.trackmatic.flink.models.surfsight;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw event from Surfsight.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SurfsightEventRaw implements Serializable {

    private String type;

    private EventData data;

    private TmMetadata meta;

    private String formattedEvent;


    public SurfsightEventRaw() {

    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public EventData getData() {
        return data;
    }

    public void setData(EventData data) {
        this.data = data;
    }

    public TmMetadata getMeta() {
        return meta;
    }

    public void setMeta(TmMetadata meta) {
        this.meta = meta;
    }

    public String getFormattedEvent() {
        return formattedEvent;
    }

    public void setFormattedEvent(String formattedEvent) {
        this.formattedEvent = formattedEvent;
    }
}
