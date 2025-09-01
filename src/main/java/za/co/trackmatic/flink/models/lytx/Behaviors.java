package za.co.trackmatic.flink.models.lytx;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a behavior configuration from Lytx data.
 * This class is used to map JSON objects that contain information
 * about driving behaviors monitored or configured in the Lytx platform.
 *
 * <p>Jackson annotations are used to ignore unknown properties during deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Behaviors implements Serializable {
    private String id;

    private String name;

    private String creationDate;

    private String revisionDate;

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

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getRevisionDate() {
        return revisionDate;
    }

    public void setRevisionDate(String revisionDate) {
        this.revisionDate = revisionDate;
    }
}

