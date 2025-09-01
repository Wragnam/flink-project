package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable data class representing raw trip data with GPS and vehicle state information.
 * <p>
 * Use the {@link Builder} to construct instances.
 */
public class RawTripData implements Serializable {

    // ---------- fields ----------
    private String serial;
    private double latitude;
    private double longitude;
    private double speed;
    private String dateFormat;
    private String dateString;
    private Double direction;
    private TmMetadata metadata;
    private long utcTimeValue;
    private Boolean ignition;
    private String ignitionString;
    private String source;
    private Integer batteryLevel;
    private Double accuracy;
    private Integer satNum;

    /**
     * Default no-argument constructor needed for Flink serialization.
     */
    @SuppressWarnings("unused")
    public RawTripData() {
    }

    /**
     * Private constructor used by {@link Builder}.
     *
     * @param b the builder instance
     */
    RawTripData(Builder b) {
        this.serial = Objects.requireNonNull(b.serial, "serial");
        this.latitude = b.latitude;
        this.longitude = b.longitude;
        this.speed = b.speed;
        this.dateFormat = b.dateFormat;
        this.dateString = b.dateString;
        this.direction = b.direction;
        this.metadata = b.metadata;
        this.utcTimeValue = b.utcTimeValue;
        this.ignition = b.ignition;
        this.ignitionString = b.ignitionString;
        this.source = b.source;
        this.batteryLevel = b.batteryLevel;
        this.accuracy = b.accuracy;
        this.satNum = b.satNum;
    }

    /**
     * Builder for {@link RawTripData}.
     * <p>
     * Allows fluent construction of immutable {@code RawTripData} instances.
     */
    public static class Builder {
        private String serial;
        private double latitude;
        private double longitude;
        private double speed;
        private String dateFormat;
        private String dateString;
        private Double direction;
        private TmMetadata metadata;
        private long utcTimeValue;
        private Boolean ignition;
        private String ignitionString;
        private String source;
        private Integer batteryLevel;
        private Double accuracy;
        private Integer satNum;

        public Builder serial(String v) {
            this.serial = v;
            return this;
        }

        public Builder latitude(double v) {
            this.latitude = v;
            return this;
        }

        public Builder longitude(double v) {
            this.longitude = v;
            return this;
        }

        public Builder speed(double v) {
            this.speed = v;
            return this;
        }

        public Builder dateFormat(String v) {
            this.dateFormat = v;
            return this;
        }

        public Builder dateString(String v) {
            this.dateString = v;
            return this;
        }

        public Builder direction(Double v) {
            this.direction = v;
            return this;
        }

        public Builder metadata(TmMetadata v) {
            this.metadata = v;
            return this;
        }

        public Builder utcTimeValue(long v) {
            this.utcTimeValue = v;
            return this;
        }

        public Builder ignition(Boolean v) {
            this.ignition = v;
            return this;
        }

        public Builder ignitionString(String v) {
            this.ignitionString = v;
            return this;
        }

        public Builder source(String v) {
            this.source = v;
            return this;
        }

        public Builder batteryLevel(Integer v) {
            this.batteryLevel = v;
            return this;
        }

        public Builder accuracy(Double v) {
            this.accuracy = v;
            return this;
        }

        public Builder satNum(Integer v) {
            this.satNum = v;
            return this;
        }

        public String getSerial() {
            return serial;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public double getSpeed() {
            return speed;
        }

        public String getDateFormat() {
            return dateFormat;
        }

        public String getDateString() {
            return dateString;
        }

        public Double getDirection() {
            return direction;
        }

        public TmMetadata getMetadata() {
            return metadata;
        }

        public long getUtcTimeValue() {
            return utcTimeValue;
        }

        public Boolean getIgnition() {
            return ignition;
        }

        public String getIgnitionString() {
            return ignitionString;
        }

        public String getSource() {
            return source;
        }

        public Integer getBatteryLevel() {
            return batteryLevel;
        }

        public Double getAccuracy() {
            return accuracy;
        }

        public Integer getSatNum() {
            return satNum;
        }

        public RawTripData build() {
            return new RawTripData(this);
        }
    }

    // ---------- (4) getters only (immutability) ----------
    public String getSerial() {
        return serial;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getSpeed() {
        return speed;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getDateString() {
        return dateString;
    }

    public Double getDirection() {
        return direction;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public long getUtcTimeValue() {
        return utcTimeValue;
    }

    public Boolean getIgnition() {
        return ignition;
    }

    public String getIgnitionString() {
        return ignitionString;
    }

    public String getSource() {
        return source;
    }

    public Integer getBatteryLevel() {
        return batteryLevel;
    }

    public Double getAccuracy() {
        return accuracy;
    }

    public Integer getSatNum() {
        return satNum;
    }
}
