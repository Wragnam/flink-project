package za.co.trackmatic.flink.models.datacache;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Represents a request to check if a geographic point is within one or more geofences.
 * Contains the coordinates of the point, organization ID, and optionally a list of site IDs.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PointInGeofenceRequest {
    private double latitude;

    private double longitude;

    private String orgId;

    private List<String> siteIds;

    public PointInGeofenceRequest() {
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public List<String> getSiteIds() {
        return siteIds;
    }

    public void setSiteIds(List<String> siteIds) {
        this.siteIds = siteIds;
    }
}
