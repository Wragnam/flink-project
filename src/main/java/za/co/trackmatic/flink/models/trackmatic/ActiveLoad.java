package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents an active load with associated metadata such as asset, driver, organization, site, and stops.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActiveLoad implements Serializable {

    /** Unique identifier for the load */
    private String loadId;

    /** Identifier of the asset associated with this load */
    private String assetId;

    /** Reference string for the load */
    private String loadReference;

    /** License number of the vehicle */
    private String licenceNo;

    /** Fleet number of the vehicle */
    private String fleetNumber;

    /** Name of the driver */
    private String driverName;

    /** Organization identifier */
    private String orgId;

    /** Site identifier */
    private String siteId;

    /** List of stops related to the load */
    private List<Stop> stops;

    public String getLoadId() {
        return loadId;
    }

    public void setLoadId(String loadId) {
        this.loadId = loadId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
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

    public List<Stop> getStops() {
        return stops;
    }

    public void setStops(List<Stop> stops) {
        this.stops = stops;
    }

    public String getLoadReference() {
        return loadReference;
    }

    public void setLoadReference(String loadReference) {
        this.loadReference = loadReference;
    }

    public String getLicenceNo() {
        return licenceNo;
    }

    public void setLicenceNo(String licenceNo) {
        this.licenceNo = licenceNo;
    }

    public String getFleetNumber() {
        return fleetNumber;
    }

    public void setFleetNumber(String fleetNumber) {
        this.fleetNumber = fleetNumber;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }
}
