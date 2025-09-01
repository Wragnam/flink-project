package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a container for temperature-related data used in generating temperature events.
 * This model includes setpoint, return air, and discharge air temperatures from different sensors,
 * as well as location, metadata, and source details.
 */
public class TempEventParameters implements Serializable {

    /**
     * Constructs a new {@code TempEventParameters} object with all required data.
     *
     * @param s1         Setpoint temperature from sensor 1
     * @param s2         Setpoint temperature from sensor 2
     * @param s3         Setpoint temperature from sensor 3
     * @param d1         Discharge air temperature from sensor 1
     * @param d2         Discharge air temperature from sensor 2
     * @param d3         Discharge air temperature from sensor 3
     * @param r1         Return air temperature from sensor 1
     * @param r2         Return air temperature from sensor 2
     * @param r3         Return air temperature from sensor 3
     * @param lat        Latitude of the asset
     * @param lon        Longitude of the asset
     * @param created    Timestamp of event creation (epoch millis)
     * @param serial     Serial number of the reefer or asset
     * @param meta       Metadata associated with the device or vehicle
     * @param source     Data source name (e.g. THERMOKING, TRACKMATIC)
     * @param isTempEvent Whether this event contains valid temperature readings
     * @param server     Server identifier or environment origin
     */
    public TempEventParameters(Double s1, Double s2, Double s3, Double d1, Double d2,
                               Double d3, Double r1, Double r2, Double r3, double lat,
                               double lon, long created, String serial, TmMetadata meta, String source,
                                boolean isTempEvent, String server){
        this.setPoint1 = s1;
        this.setPoint2 = s2;
        this.setPoint3 = s3;
        this.dischargeAir1 = d1;
        this.dischargeAir2 = d2;
        this.dischargeAir3 = d3;
        this.returnAir1 = r1;
        this.returnAir2 = r2;
        this.returnAir3 = r3;
        this.latitude = lat;
        this.longitude = lon;
        this.metadata = meta;
        this.source = source;
        this.serial = serial;
        this.created = created;
        this.isTempEvent = isTempEvent;
        this.server = server;
    }

    private Double setPoint1;

    private Double setPoint2;

    private Double setPoint3;

    private Double dischargeAir1;

    private Double dischargeAir2;

    private Double dischargeAir3;

    private Double returnAir1;

    private Double returnAir2;

    private Double returnAir3;

    private double latitude;

    private double longitude;

    private TmMetadata metadata;

    private String source;

    private String server;

    private String serial;

    private long created;

    private boolean isTempEvent;

    public Double getSetPoint1() {
        return setPoint1;
    }

    public void setSetPoint1(Double setPoint1) {
        this.setPoint1 = setPoint1;
    }

    public Double getSetPoint2() {
        return setPoint2;
    }

    public void setSetPoint2(Double setPoint2) {
        this.setPoint2 = setPoint2;
    }

    public Double getSetPoint3() {
        return setPoint3;
    }

    public void setSetPoint3(Double setPoint3) {
        this.setPoint3 = setPoint3;
    }

    public Double getDischargeAir1() {
        return dischargeAir1;
    }

    public void setDischargeAir1(Double dischargeAir1) {
        this.dischargeAir1 = dischargeAir1;
    }

    public Double getDischargeAir2() {
        return dischargeAir2;
    }

    public void setDischargeAir2(Double dischargeAir2) {
        this.dischargeAir2 = dischargeAir2;
    }

    public Double getDischargeAir3() {
        return dischargeAir3;
    }

    public void setDischargeAir3(Double dischargeAir3) {
        this.dischargeAir3 = dischargeAir3;
    }

    public Double getReturnAir1() {
        return returnAir1;
    }

    public void setReturnAir1(Double returnAir1) {
        this.returnAir1 = returnAir1;
    }

    public Double getReturnAir2() {
        return returnAir2;
    }

    public void setReturnAir2(Double returnAir2) {
        this.returnAir2 = returnAir2;
    }

    public Double getReturnAir3() {
        return returnAir3;
    }

    public void setReturnAir3(Double returnAir3) {
        this.returnAir3 = returnAir3;
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

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public boolean isTempEvent() {
        return isTempEvent;
    }

    public void setTempEvent(boolean tempEvent) {
        isTempEvent = tempEvent;
    }
}
