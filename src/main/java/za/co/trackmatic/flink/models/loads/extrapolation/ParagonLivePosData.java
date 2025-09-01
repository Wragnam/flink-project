package za.co.trackmatic.flink.models.loads.extrapolation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents live position data from Paragon including GPS details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ParagonLivePosData implements Serializable {

    public ParagonLivePosData(){}
    private String source;

    private String address;

    private Gps gps;

    private String assetId;

    private String orgId;

    private String status;

    private String deviceId;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Gps getGps() {
        return gps;
    }

    public void setGps(Gps gps) {
        this.gps = gps;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Gps implements Serializable{

        @Override
        public String toString() {
            return "Gps{" +
                    "accuracy=" + accuracy +
                    ", dateTime='" + dateTime + '\'' +
                    ", direction=" + direction +
                    ", lat=" + lat +
                    ", lng=" + lng +
                    ", optional=" + optional +
                    ", speed=" + speed +
                    '}';
        }

        private double accuracy;

        private String dateTime;

        private double direction;

        private double lat;

        private double lng;

        private Object optional;

        private double speed;

        public double getAccuracy() {
            return accuracy;
        }

        public void setAccuracy(double accuracy) {
            this.accuracy = accuracy;
        }

        public String getDateTime() {
            return dateTime;
        }

        public void setDateTime(String dateTime) {
            this.dateTime = dateTime;
        }

        public double getDirection() {
            return direction;
        }

        public void setDirection(double direction) {
            this.direction = direction;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(double lng) {
            this.lng = lng;
        }

        public Object getOptional() {
            return optional;
        }

        public void setOptional(Object optional) {
            this.optional = optional;
        }

        public double getSpeed() {
            return speed;
        }

        public void setSpeed(double speed) {
            this.speed = speed;
        }
    }
}
