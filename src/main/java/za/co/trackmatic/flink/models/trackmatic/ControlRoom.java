package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a Control Room entity with associated organization, classification types,
 * sites, and classification type sources. Used to assign events to control rooms.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ControlRoom implements Serializable {

    /** Unique identifier of the control room */
    private String id;

    /** Organization ID to which the control room belongs */
    private String orgId;

    /** Name of the control room */
    private String name;

    /** List of classification types relevant to this control room */
    private List<String> classificationTypes;

    /** List of site IDs associated with this control room */
    private List<String> siteIds;

    /** List of classification type sources associated with this control room */
    private List<ClassificationTypeSources> classificationTypeSources;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getClassificationTypes() {
        return classificationTypes;
    }

    public void setClassificationTypes(List<String> classificationTypes) {
        this.classificationTypes = classificationTypes;
    }

    public List<String> getSiteIds() {
        return siteIds;
    }

    public void setSiteIds(List<String> siteIds) {
        this.siteIds = siteIds;
    }

    public List<ClassificationTypeSources> getClassificationTypeSources() {
        return classificationTypeSources;
    }

    public void setClassificationTypeSources(List<ClassificationTypeSources> classificationTypeSources) {
        this.classificationTypeSources = classificationTypeSources;
    }
}
