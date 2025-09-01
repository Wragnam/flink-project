package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw event received from the Fleetboard system.
 * Contains detailed vehicle, driver, and event information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FleetboardEventRaw implements Serializable {

    private String id;

    private Integer eventType;

    private String timestamp;

    private String vehicleId;

    private Integer driverNameId;

    private double lat;

    private double lon;

    private String positionText;

    private Integer course;

    private Integer speed;

    private Integer mileage;

    private String gpsStatus;

    private Integer fuelLevel;

    private double operationTime;

    private Long consumption;

    private Long adBlueConsumption;

    private Long adBlueToFuelRatioConsumption;

    private Integer adBlueLevel;

    private TmMetadata metaData;

    private String formattedEvent;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getEventType() {
        return eventType;
    }

    public void setEventType(Integer eventType) {
        this.eventType = eventType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
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

    public String getPositionText() {
        return positionText;
    }

    public void setPositionText(String positionText) {
        this.positionText = positionText;
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

    public Integer getMileage() {
        return mileage;
    }

    public void setMileage(Integer mileage) {
        this.mileage = mileage;
    }

    public String getGpsStatus() {
        return gpsStatus;
    }

    public void setGpsStatus(String gpsStatus) {
        this.gpsStatus = gpsStatus;
    }

    public Integer getFuelLevel() {
        return fuelLevel;
    }

    public void setFuelLevel(Integer fuelLevel) {
        this.fuelLevel = fuelLevel;
    }

    public double getOperationTime() {
        return operationTime;
    }

    public void setOperationTime(double operationTime) {
        this.operationTime = operationTime;
    }

    public Long getConsumption() {
        return consumption;
    }

    public void setConsumption(Long consumption) {
        this.consumption = consumption;
    }

    public Long getAdBlueConsumption() {
        return adBlueConsumption;
    }

    public void setAdBlueConsumption(Long adBlueConsumption) {
        this.adBlueConsumption = adBlueConsumption;
    }

    public Long getAdBlueToFuelRatioConsumption() {
        return adBlueToFuelRatioConsumption;
    }

    public void setAdBlueToFuelRatioConsumption(Long adBlueToFuelRatioConsumption) {
        this.adBlueToFuelRatioConsumption = adBlueToFuelRatioConsumption;
    }

    public Integer getAdBlueLevel() {
        return adBlueLevel;
    }

    public void setAdBlueLevel(Integer adBlueLevel) {
        this.adBlueLevel = adBlueLevel;
    }

    public TmMetadata getMetaData() {
        return metaData;
    }

    public void setMetaData(TmMetadata metaData) {
        this.metaData = metaData;
    }

    public String getFormattedEvent() {
        return formattedEvent;
    }

    public void setFormattedEvent(String formattedEvent) {
        this.formattedEvent = formattedEvent;
    }
}
