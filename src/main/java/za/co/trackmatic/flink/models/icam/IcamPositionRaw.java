package za.co.trackmatic.flink.models.icam;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw position report from the ICAM system.
 * Contains vehicle identification, position, motion, and metadata details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcamPositionRaw implements Serializable {

    private String name;

    private long id;

    private String vin;

    private String registration_plate;

    private String brand;

    private String vehicle_class;

    private double latitude;

    private double longitude;

    private double speed;

    private double course;

    private double altitude;

    private String timestamp;

    private TmMetadata metadata;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public String getRegistration_plate() {
        return registration_plate;
    }

    public void setRegistration_plate(String registration_plate) {
        this.registration_plate = registration_plate;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getVehicle_class() {
        return vehicle_class;
    }

    public void setVehicle_class(String vehicle_class) {
        this.vehicle_class = vehicle_class;
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

    public double getCourse() {
        return course;
    }

    public void setCourse(double course) {
        this.course = course;
    }

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
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
