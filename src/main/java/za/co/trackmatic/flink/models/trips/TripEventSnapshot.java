package za.co.trackmatic.flink.models.trips;

import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;

/**
 * Represents a snapshot of an event that occurred during a trip.
 * Includes metadata such as timing, speed, and location information.
 */
public class TripEventSnapshot implements Serializable {

    /**
     * Constructs a new {@code TripEventSnapshot} with a default empty {@link Event} object.
     */
    public TripEventSnapshot() {
        this.event = new Event();
    }

    /** The type of the trip event (e.g., "SPEEDING"). */
    private String type;

    /** Unique identifier for the snapshot. */
    private String id;

    /** Maximum speed recorded during the event. */
    private double maxSpeed;

    /** Associated event details. */
    private Event event;

    /** Timestamp of the event in ISO string format. */
    private String timestamp;

    /** Optional record identifier linking this snapshot to a data source. */
    private String recordId;

    /** Timestamp of the event in milliseconds since epoch. */
    private long timestamp_millis;

    /** Address at the start of the event. */
    private String startAddress;

    /** Speed limit defined for the asset. */
    private double assetSpeedLimit;

    /** GPS coordinates at the start of the event. */
    private LatLong startLocation;

    /** Total time (in milliseconds) the vehicle was speeding. */
    private long speedingTimeMs;

    /** Start time of the event in ISO string format. */
    private String startTime;

    /** End time of the event in ISO string format. */
    private String endTime;

    /** GPS coordinates at the end of the event. */
    private LatLong endLocation;

    /** Speed limit of the road where the event occurred. */
    private double roadSpeedLimit;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(double maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public long getTimestamp_millis() {
        return timestamp_millis;
    }

    public void setTimestamp_millis(long timestamp_millis) {
        this.timestamp_millis = timestamp_millis;
    }

    public String getStartAddress() {
        return startAddress;
    }

    public void setStartAddress(String startAddress) {
        this.startAddress = startAddress;
    }

    public double getAssetSpeedLimit() {
        return assetSpeedLimit;
    }

    public void setAssetSpeedLimit(double assetSpeedLimit) {
        this.assetSpeedLimit = assetSpeedLimit;
    }

    public LatLong getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(LatLong startLocation) {
        this.startLocation = startLocation;
    }

    public long getSpeedingTimeMs() {
        return speedingTimeMs;
    }

    public void setSpeedingTimeMs(long speedingTimeMs) {
        this.speedingTimeMs = speedingTimeMs;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public LatLong getEndLocation() {
        return endLocation;
    }

    public void setEndLocation(LatLong endLocation) {
        this.endLocation = endLocation;
    }

    public double getRoadSpeedLimit() {
        return roadSpeedLimit;
    }

    public void setRoadSpeedLimit(double roadSpeedLimit) {
        this.roadSpeedLimit = roadSpeedLimit;
    }
}
