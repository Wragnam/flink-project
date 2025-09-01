package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents confirmation details related to a fuel event,
 * including exact and vehicle positions, distance between positions,
 * source of the confirmation, and whether the position is confirmed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FuelConfirmation implements Serializable {

    public FuelConfirmation(){

    }

    private double exactLat;

    private double exactLon;

    private String source;

    private String timeStamp;

    private double vehicleLat;

    private double vehicleLon;

    private boolean confirmedPosition;

    private double distanceBetween;

    public double getExactLat() {
        return exactLat;
    }

    public void setExactLat(double exactLat) {
        this.exactLat = exactLat;
    }

    public double getExactLon() {
        return exactLon;
    }

    public void setExactLon(double exactLon) {
        this.exactLon = exactLon;
    }

    public double getVehicleLat() {
        return vehicleLat;
    }

    public void setVehicleLat(double vehicleLat) {
        this.vehicleLat = vehicleLat;
    }

    public double getVehicleLon() {
        return vehicleLon;
    }

    public void setVehicleLon(double vehicleLon) {
        this.vehicleLon = vehicleLon;
    }

    public boolean isConfirmedPosition() {
        return confirmedPosition;
    }

    public void setConfirmedPosition(boolean confirmedPosition) {
        this.confirmedPosition = confirmedPosition;
    }

    public double getDistanceBetween() {
        return distanceBetween;
    }

    public void setDistanceBetween(double distanceBetween) {
        this.distanceBetween = distanceBetween;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }
}
