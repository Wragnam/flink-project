package za.co.trackmatic.flink.models.rms;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RmsLiveposRaw implements Serializable {
    private String vehiclePk;
    private String latitude;
    private String longitude;
    private String gpsDate;
    private String speed;
    private String heading;
    private String distance;
    private TmMetadata metadata;

    public String getVehiclePk() {
        return vehiclePk;
    }

    public void setVehiclePk(String vehiclePk) {
        this.vehiclePk = vehiclePk;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getGpsDate() {
        return gpsDate;
    }

    public void setGpsDate(String gpsDate) {
        this.gpsDate = gpsDate;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getHeading() {
        return heading;
    }

    public void setHeading(String heading) {
        this.heading = heading;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
