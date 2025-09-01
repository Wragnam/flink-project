package za.co.trackmatic.flink.models.mix;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw Mix Event with associated metadata and details about the event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MixEventRaw implements Serializable {

    @JsonProperty("TotalOccurances")
    private int totalOccurances;

    @JsonProperty("TotalTimeSeconds")
    private int totalTimeSeconds;

    @JsonProperty("EventTypeId")
    private long eventTypeId;

    @JsonProperty("EventId")
    private long eventId;

    @JsonProperty("DriverId")
    private long driverId;

    @JsonProperty("AssetId")
    private long assetId;

    @JsonProperty("StartPosition")
    private MixPositionRaw startPosition;

    @JsonProperty("EndPosition")
    private MixPositionRaw endPosition;

    @JsonProperty("Value")
    private double value;

    @JsonProperty("StartOdometerKilometres")
    private double startOdometerKilometres;

    @JsonProperty("EndOdometerKilometres")
    private double endOdometerKilometres;

    @JsonProperty("StartDateTime")
    private String startDateTime;

    @JsonProperty("EndDateTime")
    private String endDateTime;

    @JsonProperty("EventCategory")
    private String eventCategory;

    @JsonProperty("FuelUsedLitres")
    private double fuelUsedLitres;

    @JsonProperty("ValueType")
    private String valueType;

    @JsonProperty("ValueUnits")
    private String valueUnits;

    @JsonProperty("MediaUrls")
    private MixMediaUrl mediaUrls;

    @JsonProperty("LocationId")
    private long locationId;

    @JsonProperty("SpeedLimit")
    private double speedLimit;

    @JsonProperty("EventType")
    private String eventType;

    private TmMetadata metadata;

    private String formattedEvent;

    public int getTotalOccurances() {
        return totalOccurances;
    }

    public void setTotalOccurances(int totalOccurances) {
        this.totalOccurances = totalOccurances;
    }

    public int getTotalTimeSeconds() {
        return totalTimeSeconds;
    }

    public void setTotalTimeSeconds(int totalTimeSeconds) {
        this.totalTimeSeconds = totalTimeSeconds;
    }

    public long getEventTypeId() {
        return eventTypeId;
    }

    public void setEventTypeId(long eventTypeId) {
        this.eventTypeId = eventTypeId;
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public long getDriverId() {
        return driverId;
    }

    public void setDriverId(long driverId) {
        this.driverId = driverId;
    }

    public long getAssetId() {
        return assetId;
    }

    public void setAssetId(long assetId) {
        this.assetId = assetId;
    }

    public MixPositionRaw getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(MixPositionRaw startPosition) {
        this.startPosition = startPosition;
    }

    public MixPositionRaw getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(MixPositionRaw endPosition) {
        this.endPosition = endPosition;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public double getStartOdometerKilometres() {
        return startOdometerKilometres;
    }

    public void setStartOdometerKilometres(double startOdometerKilometres) {
        this.startOdometerKilometres = startOdometerKilometres;
    }

    public double getEndOdometerKilometres() {
        return endOdometerKilometres;
    }

    public void setEndOdometerKilometres(double endOdometerKilometres) {
        this.endOdometerKilometres = endOdometerKilometres;
    }

    public String getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(String startDateTime) {
        this.startDateTime = startDateTime;
    }

    public String getEndDateTime() {
        return endDateTime;
    }

    public void setEndDateTime(String endDateTime) {
        this.endDateTime = endDateTime;
    }

    public String getEventCategory() {
        return eventCategory;
    }

    public void setEventCategory(String eventCategory) {
        this.eventCategory = eventCategory;
    }

    public double getFuelUsedLitres() {
        return fuelUsedLitres;
    }

    public void setFuelUsedLitres(double fuelUsedLitres) {
        this.fuelUsedLitres = fuelUsedLitres;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getValueUnits() {
        return valueUnits;
    }

    public void setValueUnits(String valueUnits) {
        this.valueUnits = valueUnits;
    }

    public MixMediaUrl getMediaUrls() {
        return mediaUrls;
    }

    public void setMediaUrls(MixMediaUrl mediaUrls) {
        this.mediaUrls = mediaUrls;
    }

    public long getLocationId() {
        return locationId;
    }

    public void setLocationId(long locationId) {
        this.locationId = locationId;
    }

    public double getSpeedLimit() {
        return speedLimit;
    }

    public void setSpeedLimit(double speedLimit) {
        this.speedLimit = speedLimit;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }

    public String getFormattedEvent() {
        return formattedEvent;
    }

    public void setFormattedEvent(String formattedEvent) {
        this.formattedEvent = formattedEvent;
    }
}
