package za.co.trackmatic.flink.models.thermoking;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents Thermoking device data, including alarms, sensor readings, locations, and vehicle status.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ThermokingData implements Serializable {

    private int alarmCode;
    private String alarmDataDate;
    private String alarmDescription;
    //private Object alarmID;
    private String alarmRecommendation;
    private String alarmSeverity;
    private int alarmZone;
    private double ambientTemperature;
    //private Object avgFuelRateControllerOn;
    //private Object avgFuelRateTrip;
    //private Object axelId;

    //@JsonProperty("axle_load_sum")
    //private Object axleLoadSum;
    //private Object batteryCharge;
    //private Object batteryEquivalentCycles;
    //private Object batteryEstimatedAutonomyHours;
    //private Object batteryHealth;
    //private Object batteryHours;
    //private Object batteryNumberOfHours;
    //private Object bufferSize;
    //private Object commStatus;
    private String dataDate;
    private String databaseInsertionDate;
    private String devicePortA;
    private String devicePortB;
    private String devicePortC;
    private Double dischargeAir1;
    private Double dischargeAir2;
    private Double dischargeAir3;
    private int doorStatus;
    //private Object doorlocked;
    private boolean doorlockfitted;
    //private Object dtc_number;
    //private Object dtc_severity;
    //private Object electricalHours;
    private int engineHours;
    private int engineRpm;
    private String externalId;
    private int fuelLevel;
    private int fuelTankSize;
    //private Object geoAccessTypeDescription;
    //private Object geoFenceName;
    //private Object humidity;
    private String ignitionStatus;
    private double indSensor1;
    private double indSensor2;
    private double indSensor3;
    private double indSensor4;
    private double indSensor5;
    private double indSensor6;

    //@JsonProperty("is_coupled")
    //private Object isCoupled;
    //private Object lastIgnitionStatus;
    private double latitude;

    private List<Links> links;
    private String locationDescription;
    private double longitude;
    //private Object maintenanceHours;
    private boolean multiTemp;
    //private Object odometer;
    private String operatingMode1;
    private String operatingMode2;
    private String operatingMode3;
    //private Object operatingModeId1;
    //private Object operatingModeId2;
    //private Object operatingModeId3;
    private int packetCounter;
    private boolean powerOn;
    private String powerSource;
    private String reeferSerialNumber;
    private Double returnAir1;
    private Double returnAir2;
    private Double returnAir3;
    private String rid;
    private Double setPoint1;
    private Double setPoint2;
    private Double setPoint3;
    private int speed;
    private String stationary;
    private int totalHours;
    //    private Object tpSensor1Type;
//    private Object tpSensor1Value;
//    private Object tpSensor1Zone;
//    private Object tpSensor2Type;
//    private Object tpSensor2Value;
//    private Object tpSensor2Zone;
//    private Object tpSensor3Type;
//    private Object tpSensor3Value;
//    private Object tpSensor3Zone;
//    private Object tpSensor4Type;
//    private Object tpSensor4Value;
//    private Object tpSensor4Zone;
//    private Object tpSensor5Type;
//    private Object tpSensor5Value;
//    private Object tpSensor5Zone;
//    private Object tpSensor6Type;
//    private Object tpSensor6Value;
//    private Object tpSensor6Zone;
//
//    @JsonProperty("trailer_VIN")
//    private Object trailerVIN;
//
//    @JsonProperty("truck_VIN")
//    private Object truckVIN;
//    private Object tyrePressureDetection;
    private boolean unitMode;
    private String unitModeDetail;
    private int vehicleId;
    private String vehicleName;
    private double voltage;
    //private Object wheelBrakeLiningState;

    @JsonProperty("wheel_brake_lining")
    private Object wheelBrakeLining;

    @JsonProperty("wheel_id")
    private Object wheelID;

    @JsonProperty("wheel_tyre_pressure")
    private Object wheelTyrePressure;
    private boolean zone1Active;
    private boolean zone1Configured;
    private boolean zone1DoorOpen;
    private boolean zone2Active;
    private boolean zone2Configured;
    private boolean zone2DoorOpen;
    private boolean zone3Active;
    private boolean zone3Configured;
    private boolean zone3DoorOpen;

    private TmMetadata tmMetadata;

    public ThermokingData() {

    }

    public int getAlarmCode() {
        return alarmCode;
    }

    public void setAlarmCode(int alarmCode) {
        this.alarmCode = alarmCode;
    }

    public String getAlarmDataDate() {
        return alarmDataDate;
    }

    public void setAlarmDataDate(String alarmDataDate) {
        this.alarmDataDate = alarmDataDate;
    }

    public String getAlarmDescription() {
        return alarmDescription;
    }

    public void setAlarmDescription(String alarmDescription) {
        this.alarmDescription = alarmDescription;
    }

    public String getAlarmRecommendation() {
        return alarmRecommendation;
    }

    public void setAlarmRecommendation(String alarmRecommendation) {
        this.alarmRecommendation = alarmRecommendation;
    }

    public String getAlarmSeverity() {
        return alarmSeverity;
    }

    public void setAlarmSeverity(String alarmSeverity) {
        this.alarmSeverity = alarmSeverity;
    }

    public int getAlarmZone() {
        return alarmZone;
    }

    public void setAlarmZone(int alarmZone) {
        this.alarmZone = alarmZone;
    }

    public double getAmbientTemperature() {
        return ambientTemperature;
    }

    public void setAmbientTemperature(double ambientTemperature) {
        this.ambientTemperature = ambientTemperature;
    }

    public String getDataDate() {
        return dataDate;
    }

    public void setDataDate(String dataDate) {
        this.dataDate = dataDate;
    }

    public String getDatabaseInsertionDate() {
        return databaseInsertionDate;
    }

    public void setDatabaseInsertionDate(String databaseInsertionDate) {
        this.databaseInsertionDate = databaseInsertionDate;
    }

    public String getDevicePortA() {
        return devicePortA;
    }

    public void setDevicePortA(String devicePortA) {
        this.devicePortA = devicePortA;
    }

    public String getDevicePortB() {
        return devicePortB;
    }

    public void setDevicePortB(String devicePortB) {
        this.devicePortB = devicePortB;
    }

    public String getDevicePortC() {
        return devicePortC;
    }

    public void setDevicePortC(String devicePortC) {
        this.devicePortC = devicePortC;
    }

    public int getDoorStatus() {
        return doorStatus;
    }

    public void setDoorStatus(int doorStatus) {
        this.doorStatus = doorStatus;
    }

    public boolean isDoorlockfitted() {
        return doorlockfitted;
    }

    public void setDoorlockfitted(boolean doorlockfitted) {
        this.doorlockfitted = doorlockfitted;
    }

    public int getEngineHours() {
        return engineHours;
    }

    public void setEngineHours(int engineHours) {
        this.engineHours = engineHours;
    }

    public int getEngineRpm() {
        return engineRpm;
    }

    public void setEngineRpm(int engineRpm) {
        this.engineRpm = engineRpm;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public int getFuelLevel() {
        return fuelLevel;
    }

    public void setFuelLevel(int fuelLevel) {
        this.fuelLevel = fuelLevel;
    }


    public int getFuelTankSize() {
        return fuelTankSize;
    }

    public void setFuelTankSize(int fuelTankSize) {
        this.fuelTankSize = fuelTankSize;
    }

    public String getIgnitionStatus() {
        return ignitionStatus;
    }

    public void setIgnitionStatus(String ignitionStatus) {
        this.ignitionStatus = ignitionStatus;
    }

    public double getIndSensor1() {
        return indSensor1;
    }

    public void setIndSensor1(double indSensor1) {
        this.indSensor1 = indSensor1;
    }

    public double getIndSensor2() {
        return indSensor2;
    }

    public void setIndSensor2(double indSensor2) {
        this.indSensor2 = indSensor2;
    }

    public double getIndSensor3() {
        return indSensor3;
    }

    public void setIndSensor3(double indSensor3) {
        this.indSensor3 = indSensor3;
    }

    public double getIndSensor4() {
        return indSensor4;
    }

    public void setIndSensor4(double indSensor4) {
        this.indSensor4 = indSensor4;
    }

    public double getIndSensor5() {
        return indSensor5;
    }

    public void setIndSensor5(double indSensor5) {
        this.indSensor5 = indSensor5;
    }

    public double getIndSensor6() {
        return indSensor6;
    }

    public void setIndSensor6(double indSensor6) {
        this.indSensor6 = indSensor6;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public List<Links> getLinks() {
        return links;
    }

    public void setLinks(List<Links> links) {
        this.links = links;
    }

    public String getLocationDescription() {
        return locationDescription;
    }

    public void setLocationDescription(String locationDescription) {
        this.locationDescription = locationDescription;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public boolean isMultiTemp() {
        return multiTemp;
    }

    public void setMultiTemp(boolean multiTemp) {
        this.multiTemp = multiTemp;
    }

    public String getOperatingMode1() {
        return operatingMode1;
    }

    public void setOperatingMode1(String operatingMode1) {
        this.operatingMode1 = operatingMode1;
    }

    public String getOperatingMode2() {
        return operatingMode2;
    }

    public void setOperatingMode2(String operatingMode2) {
        this.operatingMode2 = operatingMode2;
    }

    public String getOperatingMode3() {
        return operatingMode3;
    }

    public void setOperatingMode3(String operatingMode3) {
        this.operatingMode3 = operatingMode3;
    }

    public int getPacketCounter() {
        return packetCounter;
    }

    public void setPacketCounter(int packetCounter) {
        this.packetCounter = packetCounter;
    }

    public boolean isPowerOn() {
        return powerOn;
    }

    public void setPowerOn(boolean powerOn) {
        this.powerOn = powerOn;
    }

    public String getPowerSource() {
        return powerSource;
    }

    public void setPowerSource(String powerSource) {
        this.powerSource = powerSource;
    }

    public String getReeferSerialNumber() {
        return reeferSerialNumber;
    }

    public void setReeferSerialNumber(String reeferSerialNumber) {
        this.reeferSerialNumber = reeferSerialNumber;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public String getStationary() {
        return stationary;
    }

    public void setStationary(String stationary) {
        this.stationary = stationary;
    }

    public int getTotalHours() {
        return totalHours;
    }

    public void setTotalHours(int totalHours) {
        this.totalHours = totalHours;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public String getUnitModeDetail() {
        return unitModeDetail;
    }

    public void setUnitModeDetail(String unitModeDetail) {
        this.unitModeDetail = unitModeDetail;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getVehicleName() {
        return vehicleName;
    }

    public void setVehicleName(String vehicleName) {
        this.vehicleName = vehicleName;
    }

    public double getVoltage() {
        return voltage;
    }

    public void setVoltage(double voltage) {
        this.voltage = voltage;
    }

    public Object getWheelBrakeLining() {
        return wheelBrakeLining;
    }

    public void setWheelBrakeLining(Object wheelBrakeLining) {
        this.wheelBrakeLining = wheelBrakeLining;
    }

    public Object getWheelID() {
        return wheelID;
    }

    public void setWheelID(Object wheelID) {
        this.wheelID = wheelID;
    }

    public Object getWheelTyrePressure() {
        return wheelTyrePressure;
    }

    public void setWheelTyrePressure(Object wheelTyrePressure) {
        this.wheelTyrePressure = wheelTyrePressure;
    }

    public boolean isZone1Active() {
        return zone1Active;
    }

    public void setZone1Active(boolean zone1Active) {
        this.zone1Active = zone1Active;
    }

    public boolean isZone1Configured() {
        return zone1Configured;
    }

    public void setZone1Configured(boolean zone1Configured) {
        this.zone1Configured = zone1Configured;
    }

    public boolean isZone1DoorOpen() {
        return zone1DoorOpen;
    }

    public void setZone1DoorOpen(boolean zone1DoorOpen) {
        this.zone1DoorOpen = zone1DoorOpen;
    }

    public boolean isZone2Active() {
        return zone2Active;
    }

    public void setZone2Active(boolean zone2Active) {
        this.zone2Active = zone2Active;
    }

    public boolean isZone2Configured() {
        return zone2Configured;
    }

    public void setZone2Configured(boolean zone2Configured) {
        this.zone2Configured = zone2Configured;
    }

    public boolean isZone2DoorOpen() {
        return zone2DoorOpen;
    }

    public void setZone2DoorOpen(boolean zone2DoorOpen) {
        this.zone2DoorOpen = zone2DoorOpen;
    }

    public boolean isZone3Active() {
        return zone3Active;
    }

    public void setZone3Active(boolean zone3Active) {
        this.zone3Active = zone3Active;
    }

    public boolean isZone3Configured() {
        return zone3Configured;
    }

    public void setZone3Configured(boolean zone3Configured) {
        this.zone3Configured = zone3Configured;
    }

    public boolean isZone3DoorOpen() {
        return zone3DoorOpen;
    }

    public void setZone3DoorOpen(boolean zone3DoorOpen) {
        this.zone3DoorOpen = zone3DoorOpen;
    }

    public TmMetadata getTmMetadata() {
        return tmMetadata;
    }

    public void setTmMetadata(TmMetadata tmMetadata) {
        this.tmMetadata = tmMetadata;
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
}
