package za.co.trackmatic.flink.models.blackberry;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a geographical coordinate with latitude and longitude values.
 * <p>
 * This class is used to model a location point in GPS coordinates.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeoLocation implements Serializable {
    private double lat;

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    private double lon;
}