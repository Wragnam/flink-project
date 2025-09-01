package za.co.trackmatic.flink.models.scania;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a GNSS (Global Navigation Satellite System) position reading from a Scania vehicle.
 * Includes geographic coordinates, heading, altitude, speed, and the timestamp of the position fix.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GNSSPosition implements Serializable {
    private double latitude;

    private double longitude;

    private String heading;

    private Integer altitude;

    private double speed;

    private String positionDateTime;

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

    public String getHeading() {
        return heading;
    }

    public void setHeading(String heading) {
        this.heading = heading;
    }

    public Integer getAltitude() {
        return altitude;
    }

    public void setAltitude(Integer altitude) {
        this.altitude = altitude;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getPositionDateTime() {
        return positionDateTime;
    }

    public void setPositionDateTime(String positionDateTime) {
        this.positionDateTime = positionDateTime;
    }
}
