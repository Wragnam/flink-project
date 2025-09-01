package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents positional information of an asset, including latitude, longitude, and address.
 * <p>
 * This class is typically used to store geolocation data captured by the device.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PositionInfo implements Serializable {
    private String lat;

    @JsonProperty("long")
    private String lon;

    private String address;

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
