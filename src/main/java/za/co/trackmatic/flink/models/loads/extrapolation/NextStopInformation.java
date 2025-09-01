package za.co.trackmatic.flink.models.loads.extrapolation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents information about the next stop in a load extrapolation context.
 * This class can be used both for extrapolation request events and extrapolation result events.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NextStopInformation{

    public NextStopInformation(){}

    private String orgId;

    private String loadId;

    private String siteId;

    private String assetId;

    private String stopId;

    private String locationId;

    private UpdateLoadLoadGeoPoint position;

    private String created; //Only part of the ExtrapolationRequestEvent

    //Only part of the ExtrapolationResultEvent
    private String eventTime;

    private double estimatedDistance;

    private double estimatedTravelTime;


    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getLoadId() {
        return loadId;
    }

    public void setLoadId(String loadId) {
        this.loadId = loadId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getStopId() {
        return stopId;
    }

    public void setStopId(String stopId) {
        this.stopId = stopId;
    }

    public String getLocationId() {
        return locationId;
    }

    public void setLocationId(String locationId) {
        this.locationId = locationId;
    }

    public UpdateLoadLoadGeoPoint getPosition() {
        return position;
    }

    public void setPosition(UpdateLoadLoadGeoPoint position) {
        this.position = position;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public double getEstimatedDistance() {
        return estimatedDistance;
    }

    public void setEstimatedDistance(double estimatedDistance) {
        this.estimatedDistance = estimatedDistance;
    }

    public double getEstimatedTravelTime() {
        return estimatedTravelTime;
    }

    public void setEstimatedTravelTime(double estimatedTravelTime) {
        this.estimatedTravelTime = estimatedTravelTime;
    }
}
