package za.co.trackmatic.flink.models.trackmatic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a transport load with associated details such as driver,
 * site, durations of unplanned and excess stops, and vehicle info.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Load implements Serializable {

    /**
     * Unique identifier for the load.
     */
    private String id;

    /**
     * Identifier for the site associated with the load.
     */
    private String siteId;

    /**
     * Reference code or number for the load.
     */
    private String reference;

    /**
     * Identifier of the driver assigned to this load.
     */
    private String driverId;

    /**
     * Name of the driver assigned to this load.
     */
    private String driverName;

    /**
     * Duration in minutes of any unplanned stops during the load.
     */
    private Integer unplannedStopDuration;

    /**
     * Duration in minutes of stops exceeding allowed limits.
     */
    private Integer excessStopDuration;

    /**
     * Fleet number associated with the vehicle for the load.
     */
    private String fleetNumber;

    /**
     * Licence plate number of the vehicle.
     */
    private String licenceNumber;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public Integer getUnplannedStopDuration() {
        return unplannedStopDuration;
    }

    public void setUnplannedStopDuration(Integer unplannedStopDuration) {
        this.unplannedStopDuration = unplannedStopDuration;
    }

    public Integer getExcessStopDuration() {
        return excessStopDuration;
    }

    public void setExcessStopDuration(Integer excessStopDuration) {
        this.excessStopDuration = excessStopDuration;
    }

    public String getFleetNumber() {
        return fleetNumber;
    }

    public void setFleetNumber(String fleetNumber) {
        this.fleetNumber = fleetNumber;
    }

    public String getLicenceNumber() {
        return licenceNumber;
    }

    public void setLicenceNumber(String licenceNumber) {
        this.licenceNumber = licenceNumber;
    }
}
