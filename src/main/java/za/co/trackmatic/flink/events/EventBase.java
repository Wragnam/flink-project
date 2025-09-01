package za.co.trackmatic.flink.events;

import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.trackmatic.*;

import java.io.Serializable;

/**
 * Base class for all event types and location
 * points in the Trackmatic Flink system.
 * Contains common properties such as location, device, and metadata.
 */
public abstract class EventBase implements Serializable {

    /**
     * Third party source of the data (i.e 'Trackmatic', 'Surfsight' etc.).
     */
    private String source;

    /**
     * Speed at the time of the event in KM/h.
     */
    private Double speed;

    /**
     * Altitude at the time of the event in meters.
     */
    private Double altitude;

    /**
     * Direction or heading at the time of the event in range 0-360.
     */
    private Double direction;

    /**
     * Date string associated with the event in the format yyyy-MM-ddThh:mm:ssZ.
     */
    private String date;

    /**
     * Indicates whether ignition was on.
     */
    private Boolean ignition;

    /**
     * GPS accuracy. Can be calculated by the dividing the number of satellites by the max amount of satellites
     * if there are only those properties.
     */
    private Double accuracy;

    /**
     * Return air temperature reading.
     */
    private Double returnAir;

    /**
     * Discharge air temperature reading.
     */
    private Double dischargeAir;

    /**
     * Event timestamp in seconds since epoch.
     */
    private long timestamp;

    /**
     * Organization ID associated with the device.
     */
    private String orgId;

    /**
     * Name of the driver associated with the event.
     */
    private String driverName;

    /**
     * Timestamp of the last known status change.
     */
    private String lastStatusChange;

    /**
     * Asset ID of the vehicle or equipment.
     */
    private String assetId;

    /**
     * ID of the reporting device.
     */
    private String deviceId;

    /**
     * Serial number of the reporting device.
     */
    private String deviceSerial;

    /**
     * Timestamp when the event was processed.
     */
    private long processed;

    /**
     * Latitude of the event location.
     */
    private double latitude;

    /**
     * Longitude of the event location.
     */
    private double longitude;

    /**
     * Current status of the asset or device (i.e 'MOVING', 'IDLE', 'STOPPED')
     */
    private String status;

    /**
     * Human-readable name of the location.
     */
    private String locationName;

    /**
     * ID of the known or registered location.
     */
    private String locationId;

    /**
     * Constructs an empty {@code EventBase}.
     */
    public EventBase() {
    }

    /**
     * Constructs an {@code EventBase} with basic location and source information.
     *
     * @param latitude  the latitude
     * @param longitude the longitude
     * @param source    the data source
     */
    public EventBase(double latitude, double longitude, String source) {
        this.source = source;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getLastStatusChange() {
        return lastStatusChange;
    }

    public void setLastStatusChange(String lastStatusChange) {
        this.lastStatusChange = lastStatusChange;
    }

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

    public String getDeviceSerial() {
        return deviceSerial;
    }

    public void setDeviceSerial(String deviceSerial) {
        this.deviceSerial = deviceSerial;
    }

    public long getProcessed() {
        return processed;
    }

    public void setProcessed(long processed) {
        this.processed = processed;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public String getLocationId() {
        return locationId;
    }

    public void setLocationId(String locationId) {
        this.locationId = locationId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getDirection() {
        return direction;
    }

    public void setDirection(Double direction) {
        this.direction = direction;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Boolean getIgnition() {
        return ignition;
    }

    public void setIgnition(Boolean ignition) {
        this.ignition = ignition;
    }

    public Double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(Double accuracy) {
        this.accuracy = accuracy;
    }

    public Double getReturnAir() {
        return returnAir;
    }

    public void setReturnAir(Double returnAir) {
        this.returnAir = returnAir;
    }

    public Double getDischargeAir() {
        return dischargeAir;
    }

    public void setDischargeAir(Double dischargeAir) {
        this.dischargeAir = dischargeAir;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Populates common event metadata fields from the given values.
     *
     * @param deviceId     the raw device ID
     * @param metadata     the telemetry metadata
     * @param deviceSerial the cleaned device serial number
     */
    public void populateBasicData(String deviceId, TmMetadata metadata, String deviceSerial) {
        this.setDeviceId(Utils.cleanSerial(deviceId));
        this.setDeviceSerial(deviceSerial);
        this.setOrgId(metadata.getOrgId());
        this.setAssetId(metadata.getAssetId());
        this.setProcessed(Utils.getCurrentTimestamp());
        this.setDriverName(metadata.getActiveLoad() != null ? metadata.getActiveLoad().getDriverName() : null);
    }
}
