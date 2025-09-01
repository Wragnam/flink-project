package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a geographic coordinate with latitude and longitude.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LatLong implements Serializable {

    /**
     * Latitude coordinate.
     */
    private double lat;

    /**
     * Longitude coordinate.
     */
    private double lng;

    public LatLong() {

    }

    public LatLong(double lat, double lng) {
        this.lat = lat;
        this.lng = lng;
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
}
