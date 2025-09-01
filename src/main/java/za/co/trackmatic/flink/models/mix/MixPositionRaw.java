package za.co.trackmatic.flink.models.mix;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a position record in the Mix event data model,
 * including GPS coordinates, timestamps, and related metadata.
 * Supports JSON serialization/deserialization with unknown properties ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MixPositionRaw implements Serializable {

    @JsonProperty("Timestamp")
    private String timestamp;

    @JsonProperty("Longitude")
    private double longitude;

    @JsonProperty("Latitude")
    private double latitude;

    @JsonProperty("DriverId")
    private long driverId;

    @JsonProperty("AssetId")
    private long assetId;

    @JsonProperty("PositionId")
    private long positionId;

    @JsonProperty("IsAvl")
    private boolean avl;

    @JsonProperty("Source")
    private String source;

    @JsonProperty("OdometerKilometres")
    private double odometerKilometres;

    @JsonProperty("DistanceSinceReadingKilometres")
    private double distanceSinceReadingKilometres;

    @JsonProperty("AgeOfReadingSeconds")
    private long ageOfReadingSeconds;

    @JsonProperty("Pdop")
    private int pDop;

    @JsonProperty("Vdop")
    private int vDop;

    @JsonProperty("Heading")
    private double heading;

    @JsonProperty("NumberOfSatellites")
    private int numberOfSatellites;

    @JsonProperty("AltitudeMetres")
    private double altitudeMetres;

    @JsonProperty("SpeedKilometresPerHour")
    private double speedKilometresPerHour;

    @JsonProperty("FormattedAddress")
    private String formattedAddress;

    @JsonProperty("SpeedLimit")
    private double speedLimit;

    private TmMetadata metadata;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
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

    public long getPositionId() {
        return positionId;
    }

    public void setPositionId(long positionId) {
        this.positionId = positionId;
    }

    public boolean isAvl() {
        return avl;
    }

    public void setAvl(boolean avl) {
        this.avl = avl;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public double getOdometerKilometres() {
        return odometerKilometres;
    }

    public void setOdometerKilometres(double odometerKilometres) {
        this.odometerKilometres = odometerKilometres;
    }

    public double getDistanceSinceReadingKilometres() {
        return distanceSinceReadingKilometres;
    }

    public void setDistanceSinceReadingKilometres(double distanceSinceReadingKilometres) {
        this.distanceSinceReadingKilometres = distanceSinceReadingKilometres;
    }

    public long getAgeOfReadingSeconds() {
        return ageOfReadingSeconds;
    }

    public void setAgeOfReadingSeconds(long ageOfReadingSeconds) {
        this.ageOfReadingSeconds = ageOfReadingSeconds;
    }

    public int getpDop() {
        return pDop;
    }

    public void setpDop(int pDop) {
        this.pDop = pDop;
    }

    public int getvDop() {
        return vDop;
    }

    public void setvDop(int vDop) {
        this.vDop = vDop;
    }

    public double getHeading() {
        return heading;
    }

    public void setHeading(double heading) {
        this.heading = heading;
    }

    public int getNumberOfSatellites() {
        return numberOfSatellites;
    }

    public void setNumberOfSatellites(int numberOfSatellites) {
        this.numberOfSatellites = numberOfSatellites;
    }

    public double getAltitudeMetres() {
        return altitudeMetres;
    }

    public void setAltitudeMetres(double altitudeMetres) {
        this.altitudeMetres = altitudeMetres;
    }

    public double getSpeedKilometresPerHour() {
        return speedKilometresPerHour;
    }

    public void setSpeedKilometresPerHour(double speedKilometresPerHour) {
        this.speedKilometresPerHour = speedKilometresPerHour;
    }

    public String getFormattedAddress() {
        return formattedAddress;
    }

    public void setFormattedAddress(String formattedAddress) {
        this.formattedAddress = formattedAddress;
    }

    public double getSpeedLimit() {
        return speedLimit;
    }

    public void setSpeedLimit(double speedLimit) {
        this.speedLimit = speedLimit;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
