package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents contextual information related to a trip.
 * <p>
 * This includes asset, device, load, operator, organization,
 * site, and trip identifiers used to associate telemetry data
 * with a specific operational context.
 * </p>
 */
public class Context implements Serializable {

    public Context() {
    }

    /**
     * The ID of the asset (e.g., vehicle) involved in the trip.
     */
    private String assetId;

    /**
     * The ID of the device that reported the data.
     */
    private String deviceId;

    /**
     * The ID of the load being transported, if applicable.
     */
    private String loadId;

    /**
     * The ID of the operator (driver) of the vehicle.
     */
    private String operatorId;

    /**
     * The ID of the organization to which the asset belongs.
     */
    private String orgId;

    /**
     * The ID of the site or location relevant to the trip.
     */
    private String siteId;

    /**
     * The unique ID of the trip.
     */
    private String tripId;

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getLoadId() {
        return loadId;
    }

    public void setLoadId(String loadId) {
        this.loadId = loadId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }
}
