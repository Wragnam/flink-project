package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a geographic position and associated metadata for a fleet vehicle.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Position implements Serializable {
    private String fleetId;
    private String vehicleId;
    private String driverNameId;

    private String timestamp;
    private double lon;

    private double lat;

    private String posText;

    private int course;

    private int speed;

    private int km;

    private List<State> states;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class State implements Serializable{
        private String name;
        private String timestamp;
        private String value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public String getFleetID() {
        return fleetId;
    }

    public void setFleetID(String fleetID) {
        this.fleetId = fleetID;
    }

    public String getVehicleID() {
        return vehicleId;
    }

    public void setVehicleID(String vehicleID) {
        this.vehicleId = vehicleID;
    }

    public String getDriverNameID() {
        return driverNameId;
    }

    public void setDriverNameID(String driverNameID) {
        this.driverNameId = driverNameID;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
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

    public String getPosText() {
        return posText;
    }

    public void setPosText(String posText) {
        this.posText = posText;
    }

    public int getCourse() {
        return course;
    }

    public void setCourse(int course) {
        this.course = course;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public int getKm() {
        return km;
    }

    public void setKm(int km) {
        this.km = km;
    }

    public List<State> getStates() {
        return states;
    }

    public void setStates(List<State> states) {
        this.states = states;
    }
}