package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a message containing geospatial data and metadata for a trip.
 * Tracks computed trip values, context, driver details, events, and a list of
 * geospatial snapshots collected during the trip.
 */
public class TripGeoMessage implements Serializable {

    /**
     * Initializes a new TripGeoMessage with default computed values, empty snapshots list,
     * and fresh driver details.
     */
    public TripGeoMessage() {
        ComputedTripValues initialValues = new ComputedTripValues(0, 0, 0, 0, 0, 0,0);
        this.snapshots = new ArrayList<>();
        this.setComputedValues(initialValues);
        this.driverDetails = new DriverDetails();
        totalSpeed =0;
        steps =0;
    }

    /** Computed statistics for the trip such as average speed, driving time, etc. */
    private ComputedTripValues computedValues;

    /** Contextual identifiers related to the trip, device, asset, etc. */
    private Context context = new Context();

    /** Details about the driver involved in the trip. */
    private DriverDetails driverDetails;

    /** Starting event/location of the trip. */
    private EventLocation startEvent;

    /** Ending event/location of the trip. */
    private EventLocation endEvent;

    /** Identifier summarizing the events of the trip. */
    private String eventSummaryId;

    /** Unique identifier for this trip geo message. */
    private String id;

    /** List of geospatial snapshots captured during the trip. */
    private List<TripGeoSnapshot> snapshots;

    /** Accumulated total speed value (sum of speeds recorded). For calculation purposes. */
    private double totalSpeed;

    /** Count of steps or data points recorded. For calculation purposes. */
    private long steps;

    /** Last sequence number processed for the trip. */
    private Integer lastSeq;

    /** ID of the last trip processed. */
    private Long lastTripId;

    /** Timestamp of the last processed event. */
    private Long lastTime;

    /** Latitude of the last recorded position. */
    private Double lastLat;

    /** Longitude of the last recorded position. */
    private Double lastLon;

    /** Source identifier for the message origin. */
    private String source;

    /** Flag indicating if the trip is currently active. */
    private boolean activeTrip;

    public Integer getLastSeq() {
        return lastSeq;
    }

    public void setLastSeq(Integer lastSeq) {
        this.lastSeq = lastSeq;
    }

    public double getTotalSpeed() {
        return totalSpeed;
    }

    public void setTotalSpeed(double totalSpeed) {
        this.totalSpeed = totalSpeed;
    }

    public void increaseTotalSpeed(double speed){
        this.totalSpeed += speed;
    }

    public long getSteps() {
        return steps;
    }

    public void setSteps(long steps) {
        this.steps = steps;
    }

    public void incrementSteps(){
        this.steps++;
    }

    public void incrementTotalSpeed(double speed){
        this.totalSpeed += speed;
    }

    public ComputedTripValues getComputedValues() {
        return computedValues;
    }

    public void setComputedValues(ComputedTripValues computedValues) {
        this.computedValues = computedValues;
    }

    public EventLocation getStartEvent() {
        return startEvent;
    }

    public void setStartEvent(EventLocation startEvent) {
        this.startEvent = startEvent;
    }

    public List<TripGeoSnapshot> getSnapshots() {
        return snapshots;
    }

    public void setSnapshots(List<TripGeoSnapshot> snapshots) {
        this.snapshots = snapshots;
    }

    public void appendSnapshots(TripGeoSnapshot tripGeoSnapshot) {
        this.snapshots.add(tripGeoSnapshot);
    }


    public EventLocation getEndEvent() {
        return endEvent;
    }

    public void setEndEvent(EventLocation endEvent) {
        this.endEvent = endEvent;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public DriverDetails getDriverDetails() {
        return driverDetails;
    }

    public void setDriverDetails(DriverDetails driverDetails) {
        this.driverDetails = driverDetails;
    }

    public String getEventSummaryId() {
        return eventSummaryId;
    }

    public void setEventSummaryId(String eventSummaryId) {
        this.eventSummaryId = eventSummaryId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getLastTripId() {
        return lastTripId;
    }

    public void setLastTripId(Long lastTripId) {
        this.lastTripId = lastTripId;
    }

    public Long getLastTime() {
        return lastTime;
    }

    public void setLastTime(Long lastTime) {
        this.lastTime = lastTime;
    }

    public Double getLastLat() {
        return lastLat;
    }

    public void setLastLat(Double lastLat) {
        this.lastLat = lastLat;
    }

    public Double getLastLon() {
        return lastLon;
    }

    public void setLastLon(Double lastLon) {
        this.lastLon = lastLon;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public boolean isActiveTrip() {
        return activeTrip;
    }

    public void setActiveTrip(boolean activeTrip) {
        this.activeTrip = activeTrip;
    }
}
