package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the GNSS (Global Navigation Satellite System) position information of a Volvo vehicle.
 * Contains geographic coordinates, heading, altitude, speed, and the timestamp of the position.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoGnssPosition implements Serializable {
    private double latitude;

    private double longitude;

    private Integer heading;

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

    public Integer getHeading() {
        return heading;
    }

    public void setHeading(Integer heading) {
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
