package za.co.trackmatic.flink.models.surfsight;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Abstract base class representing common data properties for Surfsight devices.
 * Includes metadata, geolocation, and movement information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class DataBase implements Serializable  {
    private TmMetadata meta;

    private long id;

    private String serialNumber;

    private long time;

    private double lat;

    private double lon;

    private double alt;

    private double speed;

    public DataBase() {

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
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

    public double getAlt() {
        return alt;
    }

    public void setAlt(double alt) {
        this.alt = alt;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public TmMetadata getMeta() {
        return meta;
    }

    public void setMeta(TmMetadata meta) {
        this.meta = meta;
    }
}
