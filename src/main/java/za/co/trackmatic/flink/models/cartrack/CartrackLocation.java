package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the location information of a vehicle in the Cartrack system.
 * Includes geographical coordinates, GPS fix type, position description,
 * and associated geofence IDs.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackLocation implements Serializable {
    private String updated;
    private Double longitude;
    private Double latitude;

    @JsonProperty("gps_fix_type")
    private Integer gpsFixType;

    @JsonProperty("position_description")
    private String positionDescription;

    @JsonProperty("geofence_ids")
    private List<String> geofenceIds;

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(String updated) {
        this.updated = updated;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Integer getGpsFixType() {
        return gpsFixType;
    }

    public void setGpsFixType(Integer gpsFixType) {
        this.gpsFixType = gpsFixType;
    }

    public String getPositionDescription() {
        return positionDescription;
    }

    public void setPositionDescription(String positionDescription) {
        this.positionDescription = positionDescription;
    }

    public List<String> getGeofenceIds() {
        return geofenceIds;
    }

    public void setGeofenceIds(List<String> geofenceIds) {
        this.geofenceIds = geofenceIds;
    }
}
