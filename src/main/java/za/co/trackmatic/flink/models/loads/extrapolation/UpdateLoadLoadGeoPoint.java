package za.co.trackmatic.flink.models.loads.extrapolation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a geographical point with latitude and longitude coordinates,
 * typically used to update load location information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpdateLoadLoadGeoPoint implements Serializable {

    private double lat;

    private double lng;

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
