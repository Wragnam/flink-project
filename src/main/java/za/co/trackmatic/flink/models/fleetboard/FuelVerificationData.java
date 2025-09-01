package za.co.trackmatic.flink.models.fleetboard;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the data related to fuel verification including position, trace,
 * transaction identifier, and latitude/longitude coordinates.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FuelVerificationData implements Serializable {
    private Position position;
    private PositionTrace trace;

    private String transactionId;

    private double lat;

    private double lon;

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public PositionTrace getTrace() {
        return trace;
    }

    public void setTrace(PositionTrace trace) {
        this.trace = trace;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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
}
