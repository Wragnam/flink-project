package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.models.paragon.DataItem;

import java.io.Serializable;

/**
 * Represents raw live position data extending {@link RawTripData} with additional sensor and state information.
 */
public class RawLiveposData extends RawTripData implements Serializable {
    private String deviceId;
    private Double dischargeAir;
    private Double returnAir;
    private Boolean panic;
    private Double mainBatteryLevel;
    private Double backupBatteryLevel;
    private DataItem.Cdd cdd;
    private Double setPoint1;
    private Double setPoint2;
    private Double setPoint3;
    private Boolean rearDoorOpen;
    private Boolean sideDoorOpen;
    private Boolean zone1DoorOpen;
    private Boolean zone2DoorOpen;
    private Boolean zone3DoorOpen;
    private Double fuelLevel;
    private Integer fuelTankSize;
    private long created;

    /**
     * Private constructor to enforce the use of Builder.
     *
     * @param builder the builder to copy values from
     */
    private RawLiveposData(Builder builder) {
        super(builder); // call RawTripData.Builder constructor
        this.deviceId = builder.deviceId;
        this.dischargeAir = builder.dischargeAir;
        this.returnAir = builder.returnAir;
        this.panic = builder.panic;
        this.mainBatteryLevel = builder.mainBatteryLevel;
        this.backupBatteryLevel = builder.backupBatteryLevel;
        this.cdd = builder.cdd;
        this.setPoint1 = builder.setPoint1;
        this.setPoint2 = builder.setPoint2;
        this.setPoint3 = builder.setPoint3;
        this.rearDoorOpen = builder.rearDoorOpen;
        this.sideDoorOpen = builder.sideDoorOpen;
        this.zone1DoorOpen = builder.zone1DoorOpen;
        this.zone2DoorOpen = builder.zone2DoorOpen;
        this.zone3DoorOpen = builder.zone3DoorOpen;
        this.fuelLevel = builder.fuelLevel;
        this.fuelTankSize = builder.fuelTankSize;
        this.created = builder.created;
    }

    /**
     * Builder for {@link RawLiveposData}.
     */
    public static class Builder extends RawTripData.Builder {
        private String deviceId;
        private Double dischargeAir;
        private Double returnAir;
        private Boolean panic;
        private Double mainBatteryLevel;
        private Double backupBatteryLevel;
        private DataItem.Cdd cdd;
        private Double setPoint1;
        private Double setPoint2;
        private Double setPoint3;
        private Boolean rearDoorOpen;
        private Boolean sideDoorOpen;
        private Boolean zone1DoorOpen;
        private Boolean zone2DoorOpen;
        private Boolean zone3DoorOpen;
        private Double fuelLevel;
        private Integer fuelTankSize;
        private long created;

        /**
         * Creates a builder copying all fields from the given base {@link RawTripData.Builder}.
         *
         * @param base the base builder to copy from
         */
        public Builder(RawTripData.Builder base) {
            // Manually copy all fields from base into this builder
            this.serial(base.getSerial());
            this.latitude(base.getLatitude());
            this.longitude(base.getLongitude());
            this.speed(base.getSpeed());
            this.dateFormat(base.getDateFormat());
            this.dateString(base.getDateString());
            this.direction(base.getDirection());
            this.metadata(base.getMetadata());
            this.utcTimeValue(base.getUtcTimeValue());
            this.ignition(base.getIgnition());
            this.ignitionString(base.getIgnitionString());
            this.source(base.getSource());
            this.batteryLevel(base.getBatteryLevel());
            this.accuracy(base.getAccuracy());
            this.satNum(base.getSatNum());
        }

        public Builder deviceId(String v) {
            this.deviceId = v;
            return this;
        }

        public Builder dischargeAir(Double v) {
            this.dischargeAir = v;
            return this;
        }

        public Builder returnAir(Double v) {
            this.returnAir = v;
            return this;
        }

        public Builder panic(Boolean v) {
            this.panic = v;
            return this;
        }

        public Builder mainBatteryLevel(Double v) {
            this.mainBatteryLevel = v;
            return this;
        }

        public Builder backupBatteryLevel(Double v) {
            this.backupBatteryLevel = v;
            return this;
        }

        public Builder cdd(DataItem.Cdd v) {
            this.cdd = v;
            return this;
        }

        public Builder setPoint1(Double v) {
            this.setPoint1 = v;
            return this;
        }

        public Builder setPoint2(Double v) {
            this.setPoint2 = v;
            return this;
        }

        public Builder setPoint3(Double v) {
            this.setPoint3 = v;
            return this;
        }

        public Builder rearDoorOpen(Boolean v) {
            this.rearDoorOpen = v;
            return this;
        }

        public Builder sideDoorOpen(Boolean v) {
            this.sideDoorOpen = v;
            return this;
        }

        public Builder fuelLevel(Double v) {
            this.fuelLevel = v;
            return this;
        }

        public Builder fuelTankSize(Integer v) {
            this.fuelTankSize = v;
            return this;
        }

        public Builder zone1DoorOpen(Boolean v) {
            this.zone1DoorOpen = v;
            return this;
        }

        public Builder zone2DoorOpen(Boolean v) {
            this.zone2DoorOpen = v;
            return this;
        }

        public Builder zone3DoorOpen(Boolean v) {
            this.zone3DoorOpen = v;
            return this;
        }

        public Builder created(long v) {
            this.created = v;
            return this;
        }


        /**
         * Builds the {@link RawLiveposData} instance.
         *
         * @return the built {@link RawLiveposData}
         */
        @Override
        public RawLiveposData build() {
            return new RawLiveposData(this);
        }
    }

    public String getDeviceId() {
        return deviceId;
    }

    public Double getDischargeAir() {
        return dischargeAir;
    }

    public Double getReturnAir() {
        return returnAir;
    }

    public Boolean getPanic() {
        return panic;
    }

    public Double getMainBatteryLevel() {
        return mainBatteryLevel;
    }

    public Double getBackupBatteryLevel() {
        return backupBatteryLevel;
    }

    public DataItem.Cdd getCdd() {
        return cdd;
    }

    public Double getSetPoint1() {
        return setPoint1;
    }

    public Double getSetPoint2() {
        return setPoint2;
    }

    public Double getSetPoint3() {
        return setPoint3;
    }

    public Boolean getRearDoorOpen() {
        return rearDoorOpen;
    }

    public Boolean getSideDoorOpen() {
        return sideDoorOpen;
    }

    public Double getFuelLevel() {
        return fuelLevel;
    }

    public Integer getFuelTankSize() {
        return fuelTankSize;
    }

    public long getCreated() {
        return created;
    }

    public Boolean getZone1DoorOpen() {
        return zone1DoorOpen;
    }

    public Boolean getZone2DoorOpen() {
        return zone2DoorOpen;
    }

    public Boolean getZone3DoorOpen() {
        return zone3DoorOpen;
    }
}
