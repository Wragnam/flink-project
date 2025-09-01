package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a classification type and its associated list of TPS sources.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClassificationTypeSources implements Serializable {

    /** The classification type name or identifier */
    private String classificationType;

    /** List of TPS sources associated with the classification type */
    private List<String> tpsSources;

    public String getClassificationType() {
        return classificationType;
    }

    public void setClassificationType(String classificationType) {
        this.classificationType = classificationType;
    }

    public List<String> getTpsSources() {
        return tpsSources;
    }

    public void setTpsSources(List<String> tpsSources) {
        this.tpsSources = tpsSources;
    }
}
