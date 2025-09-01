package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents a single geospatial snapshot within a trip,
 * containing location, movement, and device status information.
 */
public class TripGeoSnapshot implements Serializable {

    /**
     * Creates a new TripGeoSnapshot with default values.
     * Initializes the {@link Optional} field to a new instance.
     */
    public TripGeoSnapshot(){
        this.optional = new Optional();
    }

    /** The accuracy of the location measurement. */
    private Double accuracy;

    /** Indicates whether the device battery is currently charging. */
    private boolean batteryCharging;

    /** The current battery level percentage of the device. */
    private Integer batteryLevel;

    /** The timestamp of when this snapshot was taken, as a formatted string. */
    private String dateTime;

    /** The direction or heading of the asset/device, in degrees. */
    private Double direction;

    /** The latitude coordinate of the snapshot location. */
    private double lat;

    /** The longitude coordinate of the snapshot location. */
    private double lng;

    /** Additional location information such as name and ID. */
    private LocationInfo location;

    /** Optional additional parameters such as altitude and satellite count. */
    private Optional optional;

    /** The speed of the asset/device at the time of the snapshot, in units consistent with the system (e.g., km/h). */
    private double speed;

    /** Status string associated with this snapshot (e.g., "moving", "stopped"). */
    private String status;

    public Double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(Double accuracy) {
        this.accuracy = accuracy;
    }

    public boolean isBatteryCharging() {
        return batteryCharging;
    }

    public void setBatteryCharging(boolean batteryCharging) {
        this.batteryCharging = batteryCharging;
    }

    public Integer getBatteryLevel() {
        return batteryLevel;
    }

    public void setBatteryLevel(Integer batteryLevel) {
        this.batteryLevel = batteryLevel;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public Double getDirection() {
        return direction;
    }

    public void setDirection(Double direction) {
        this.direction = direction;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLng() {
        return lng;
    }

    public void setLng(double lng) {
        this.lng = lng;
    }

    public LocationInfo getLocation() {
        return location;
    }

    public void setLocation(LocationInfo location) {
        this.location = location;
    }

    public Optional getOptional() {
        return optional;
    }

    public void setOptional(Optional optional) {
        this.optional = optional;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
