package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw position event from the Cartrack system,
 * including vehicle status, location, telemetry, and associated metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackPositionRaw implements Serializable {
    @JsonProperty("vehicle_id")
    private Long vehicleId;

    private String registration;

    @JsonProperty("engine_type")
    private String engineType;

    @JsonProperty("chassis_number")
    private String chassisNumber;

    @JsonProperty("event_ts")
    private String eventTs;

    private Integer bearing;

    private Integer speed;

    private Boolean ignition;

    private Boolean idling;

    private Integer odometer;

    private Integer clock;

    private Integer altitude;

    private Integer rpm;

    @JsonProperty("road_speed")
    private Integer roadSpeed;

    private String vext;

    private Double temp1;

    private Double temp2;

    private Double temp3;

    private Double temp4;

    @JsonProperty("last_identification_tag_id")
    private String lastIdentificationTagId;

    @JsonProperty("io_panic")
    private String ioPanic;

    @JsonProperty("io_disarm")
    private String ioDisarm;

    private Integer dynamic1;

    private Integer dynamic2;

    private Integer dynamic3;

    private Integer dynamic4;

    @JsonProperty("input_state")
    private Integer inputState;

    @JsonProperty("input_state2")
    private Integer inputState2;

    @JsonProperty("input_state3")
    private Integer inputState3;

    @JsonProperty("central_locking_status")
    private Boolean centralLockingStatus;

    @JsonProperty("tcu_percentage")
    private Integer tcuBatteryPercentage;

    private CartrackDriver driver;

    private CartrackFuel fuel;

    private CartrackElectric electric;

    private CartrackLocation location;

    private TmMetadata metadata;

    public Long getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(Long vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getRegistration() {
        return registration;
    }

    public void setRegistration(String registration) {
        this.registration = registration;
    }

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public String getChassisNumber() {
        return chassisNumber;
    }

    public void setChassisNumber(String chassisNumber) {
        this.chassisNumber = chassisNumber;
    }

    public String getEventTs() {
        return eventTs;
    }

    public void setEventTs(String eventTs) {
        this.eventTs = eventTs;
    }

    public Integer getBearing() {
        return bearing;
    }

    public void setBearing(Integer bearing) {
        this.bearing = bearing;
    }

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    public Boolean getIgnition() {
        return ignition;
    }

    public void setIgnition(Boolean ignition) {
        this.ignition = ignition;
    }

    public Boolean getIdling() {
        return idling;
    }

    public void setIdling(Boolean idling) {
        this.idling = idling;
    }

    public Integer getOdometer() {
        return odometer;
    }

    public void setOdometer(Integer odometer) {
        this.odometer = odometer;
    }

    public Integer getClock() {
        return clock;
    }

    public void setClock(Integer clock) {
        this.clock = clock;
    }

    public Integer getAltitude() {
        return altitude;
    }

    public void setAltitude(Integer altitude) {
        this.altitude = altitude;
    }

    public Integer getRpm() {
        return rpm;
    }

    public void setRpm(Integer rpm) {
        this.rpm = rpm;
    }

    public Integer getRoadSpeed() {
        return roadSpeed;
    }

    public void setRoadSpeed(Integer roadSpeed) {
        this.roadSpeed = roadSpeed;
    }

    public String getVext() {
        return vext;
    }

    public void setVext(String vext) {
        this.vext = vext;
    }

    public Double getTemp1() {
        return temp1;
    }

    public void setTemp1(Double temp1) {
        this.temp1 = temp1;
    }

    public Double getTemp2() {
        return temp2;
    }

    public void setTemp2(Double temp2) {
        this.temp2 = temp2;
    }

    public Double getTemp3() {
        return temp3;
    }

    public void setTemp3(Double temp3) {
        this.temp3 = temp3;
    }

    public Double getTemp4() {
        return temp4;
    }

    public void setTemp4(Double temp4) {
        this.temp4 = temp4;
    }

    public String getLastIdentificationTagId() {
        return lastIdentificationTagId;
    }

    public void setLastIdentificationTagId(String lastIdentificationTagId) {
        this.lastIdentificationTagId = lastIdentificationTagId;
    }

    public String getIoPanic() {
        return ioPanic;
    }

    public void setIoPanic(String ioPanic) {
        this.ioPanic = ioPanic;
    }

    public String getIoDisarm() {
        return ioDisarm;
    }

    public void setIoDisarm(String ioDisarm) {
        this.ioDisarm = ioDisarm;
    }

    public Integer getDynamic1() {
        return dynamic1;
    }

    public void setDynamic1(Integer dynamic1) {
        this.dynamic1 = dynamic1;
    }

    public Integer getDynamic2() {
        return dynamic2;
    }

    public void setDynamic2(Integer dynamic2) {
        this.dynamic2 = dynamic2;
    }

    public Integer getDynamic3() {
        return dynamic3;
    }

    public void setDynamic3(Integer dynamic3) {
        this.dynamic3 = dynamic3;
    }

    public Integer getDynamic4() {
        return dynamic4;
    }

    public void setDynamic4(Integer dynamic4) {
        this.dynamic4 = dynamic4;
    }

    public Integer getInputState() {
        return inputState;
    }

    public void setInputState(Integer inputState) {
        this.inputState = inputState;
    }

    public Integer getInputState2() {
        return inputState2;
    }

    public void setInputState2(Integer inputState2) {
        this.inputState2 = inputState2;
    }

    public Integer getInputState3() {
        return inputState3;
    }

    public void setInputState3(Integer inputState3) {
        this.inputState3 = inputState3;
    }

    public Boolean getCentralLockingStatus() {
        return centralLockingStatus;
    }

    public void setCentralLockingStatus(Boolean centralLockingStatus) {
        this.centralLockingStatus = centralLockingStatus;
    }

    public Integer getTcuBatteryPercentage() {
        return tcuBatteryPercentage;
    }

    public void setTcuBatteryPercentage(Integer tcuBatteryPercentage) {
        this.tcuBatteryPercentage = tcuBatteryPercentage;
    }

    public CartrackDriver getDriver() {
        return driver;
    }

    public void setDriver(CartrackDriver driver) {
        this.driver = driver;
    }

    public CartrackFuel getFuel() {
        return fuel;
    }

    public void setFuel(CartrackFuel fuel) {
        this.fuel = fuel;
    }

    public CartrackElectric getElectric() {
        return electric;
    }

    public void setElectric(CartrackElectric electric) {
        this.electric = electric;
    }

    public CartrackLocation getLocation() {
        return location;
    }

    public void setLocation(CartrackLocation location) {
        this.location = location;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
