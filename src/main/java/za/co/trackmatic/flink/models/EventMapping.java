package za.co.trackmatic.flink.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a mapping configuration for events from a specific source,
 * where original event values are mapped to new types.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventMapping implements Serializable {

    /**
     * The source identifier for which the event mapping applies (i.e "SURFSIGHT")
     */
    private String source;

    /**
     * The list of event value mappings from original to new types.
     */
    private List<ValueMapping> events;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<ValueMapping> getEvents() {
        return events;
    }

    public void setEvents(List<ValueMapping> events) {
        this.events = events;
    }

    /**
     * Represents a mapping from an original event value to a new event type.
     */
    public static class ValueMapping implements Serializable {
        private String original;

        public String getOriginal() {
            return original;
        }

        public void setOriginal(String original) {
            this.original = original;
        }

        public String getNew_type() {
            return new_type;
        }

        public void setNew_type(String new_type) {
            this.new_type = new_type;
        }

        private String new_type;
    }
}
