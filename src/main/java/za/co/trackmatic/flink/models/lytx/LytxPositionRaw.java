package za.co.trackmatic.flink.models.lytx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw position update from the Lytx system.
 *
 * <p>This class contains vehicle position details such as latitude, longitude, speed,
 * and timestamp information. It also carries optional metadata from the Trackmatic system.</p>
 *
 * <p>Unknown JSON properties are ignored during deserialization.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LytxPositionRaw implements Serializable {

    private Long vehicleId;

    private String dcVehicleId;

    private double latitude;

    private double longitude;

    private double speed;

    private String gpsDateTime;

    private String timestamp;

    private TmMetadata metadata;

    public Long getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(Long vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getDcVehicleId() {
        return dcVehicleId;
    }

    public void setDcVehicleId(String dcVehicleId) {
        this.dcVehicleId = dcVehicleId;
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

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getGpsDateTime() {
        return gpsDateTime;
    }

    public void setGpsDateTime(String gpsDateTime) {
        this.gpsDateTime = gpsDateTime;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
