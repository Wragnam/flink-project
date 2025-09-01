package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a trace of position data for a vehicle including various timestamps.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PositionTrace implements Serializable {
    private String vehicleId;
    private String fleetId;
    private String gpsTime;
    private double lon;
    private double lat;
    private String vehicleTimestamp;
    private String serverTimestamp;

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getFleetId() {
        return fleetId;
    }

    public void setFleetId(String fleetId) {
        this.fleetId = fleetId;
    }

    public String getGpsTime() {
        return gpsTime;
    }

    public void setGpsTime(String gpsTime) {
        this.gpsTime = gpsTime;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public String getVehicleTimestamp() {
        return vehicleTimestamp;
    }

    public void setVehicleTimestamp(String vehicleTimestamp) {
        this.vehicleTimestamp = vehicleTimestamp;
    }

    public String getServerTimestamp() {
        return serverTimestamp;
    }

    public void setServerTimestamp(String serverTimestamp) {
        this.serverTimestamp = serverTimestamp;
    }
}