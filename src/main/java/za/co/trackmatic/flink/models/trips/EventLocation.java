package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents the location details where a specific event occurred.
 * <p>
 * Includes geolocation coordinates, address, timestamp, and additional metadata.
 * </p>
 */
public class EventLocation implements Serializable {

    /**
     * Human-readable address where the event occurred.
     */
    private String address;

    /**
     * Timestamp of the event in ISO 8601 string format.
     */
    private String dateTime;

    /**
     * Latitude coordinate of the event location.
     */
    private double lat;

    /**
     * Longitude coordinate of the event location.
     */
    private double lng;

    /**
     * Additional location metadata such as region, zone, or accuracy.
     */
    private LocationInfo location;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
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
}
