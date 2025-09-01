package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a raw position event from Fleetboard, including vehicle, driver, and location data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FleetboardPositionRaw implements Serializable {

    private String fleetId;

    private String vehicleId;

    private Integer driverNameId;

    private String timestamp;

    private double lat;

    private double lon;

    private String posText;

    private Integer course;

    private Integer speed;

    private Integer km;

    private List<State> states;

    private TmMetadata metaData;

    public String getFleetId() {
        return fleetId;
    }

    public void setFleetId(String fleetId) {
        this.fleetId = fleetId;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public Integer getDriverNameId() {
        return driverNameId;
    }

    public void setDriverNameId(Integer driverNameId) {
        this.driverNameId = driverNameId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

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

    public String getPosText() {
        return posText;
    }

    public void setPosText(String posText) {
        this.posText = posText;
    }

    public Integer getCourse() {
        return course;
    }

    public void setCourse(Integer course) {
        this.course = course;
    }

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    public Integer getKm() {
        return km;
    }

    public void setKm(Integer km) {
        this.km = km;
    }

    public TmMetadata getMetaData() {
        return metaData;
    }

    public void setMetaData(TmMetadata metaData) {
        this.metaData = metaData;
    }

    public List<State> getStates() {
        return states;
    }

    public void setStates(List<State> states) {
        this.states = states;
    }
}
