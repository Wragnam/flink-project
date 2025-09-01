package za.co.trackmatic.flink.models.surfsight;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents an event data record for Surfsight devices,
 * extending the common properties from DataBase.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventData extends DataBase implements Serializable {
    private String eventType;

    private String other;

    private List<SurfsightCameraFiles> files;

    public EventData() {

    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    public List<SurfsightCameraFiles> getFiles() {
        return files;
    }

    public void setFiles(List<SurfsightCameraFiles> files) {
        this.files = files;
    }

}
