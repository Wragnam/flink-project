package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents a stop in a route or load, including location, timing, and duration details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Stop {

    /** Latitude of the stop location as a string. */
    private String lat;

    /** Longitude of the stop location as a string. */
    private String lng;

    /** Unique identifier of the stop. */
    private String stopId;

    /** Identifier for the associated location. */
    private String locationId;

    /** Name of the location. */
    private String locationName;

    /** Arrival timestamp at the stop (format unspecified, usually ISO string). */
    private String arrival;

    /** Departure timestamp from the stop (format unspecified, usually ISO string). */
    private String departure;

    /** Timestamp or object indicating when the first activity started at the stop (type is generic Object). */
    private Object firstActivityStarted;

    /** Duration of the stop in milliseconds or seconds (units not specified). */
    private long duration;

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
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

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public String getArrival() {
        return arrival;
    }

    public void setArrival(String arrival) {
        this.arrival = arrival;
    }

    public String getDeparture() {
        return departure;
    }

    public void setDeparture(String departure) {
        this.departure = departure;
    }

    public Object getFirstActivityStarted() {
        return firstActivityStarted;
    }

    public void setFirstActivityStarted(Object firstActivityStarted) {
        this.firstActivityStarted = firstActivityStarted;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}
