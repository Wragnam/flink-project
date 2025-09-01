package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw event from a Cartrack tracking device.
 *
 * <p>This class captures detailed telemetry and event data,
 * including GPS coordinates, vehicle status, sensor readings,
 * timestamps, and metadata related to the event.</p>
 *
 * <p>Fields are mapped to JSON properties for easy parsing with Jackson.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackEventRaw implements Serializable {

    @JsonProperty("event_id")
    private Long eventId;

    @JsonProperty("vehicle_id")
    private Long vehicleId;

    private String registration;

    @JsonProperty("chassis_number")
    private String chassisNumber;

    @JsonProperty("terminal_event_type_id")
    private Long terminalEventTypeId;

    @JsonProperty("event_description")
    private String eventDescription;

    private Double longitude;

    private Double latitude;

    @JsonProperty("linear_g")
    private Double linearG;

    @JsonProperty("lateral_g")
    private Double lateralG;

    private Integer altitude;

    private Integer odometer;

    private Integer clock;

    private Integer bearing;

    private Boolean ignition;

    private Integer speed;

    @JsonProperty("road_speed")
    private Integer roadSpeed;

    private Integer rpm;

    @JsonProperty("road_speeding")
    private Boolean roadSpeeding;

    private Double temp1;

    private Double temp2;

    private Double temp3;

    private Double temp4;

    @JsonProperty("analog_0")
    private Integer analog0;

    @JsonProperty("analog_1")
    private Integer analog1;

    @JsonProperty("analog_2")
    private Integer analog2;

    private Double adc0;

    private Double adc1;

    private Double adc2;

    @JsonProperty("position_description_id")
    private Long positionDescriptionId;

    @JsonProperty("position_description")
    private String positionDescription;

    @JsonProperty("input_state")
    private Integer inputState;

    @JsonProperty("input_state2")
    private Integer inputState2;

    @JsonProperty("input_state3")
    private Integer inputState3;

    @JsonProperty("output_state")
    private Double outputState;

    private Double vext;

    private Double vgsm;

    private Integer dynamic1;

    private Integer dynamic2;

    private Integer dynamic3;

    private Integer dynamic4;

    @JsonProperty("battery_percentage_left")
    private Integer batteryPercentageLeft;

    @JsonProperty("event_ts")
    private String eventTs;

    @JsonProperty("received_ts")
    private String receivedTs;

    @JsonProperty("z_accel")
    private Double zaccel;

    @JsonProperty("y_accel")
    private Double yaccel;

    @JsonProperty("x_accel")
    private Double xaccel;

    @JsonProperty("gps_fix_type")
    private Integer gpsFixType;

    @JsonProperty("terminal_id")
    private Long terminalId;

    @JsonProperty("user_id")
    private Long userId;

    @JsonProperty("driver_id")
    private String driverId;

    private String eventName;

    private TmMetadata metadata;

    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

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

    public String getChassisNumber() {
        return chassisNumber;
    }

    public void setChassisNumber(String chassisNumber) {
        this.chassisNumber = chassisNumber;
    }

    public Long getTerminalEventTypeId() {
        return terminalEventTypeId;
    }

    public void setTerminalEventTypeId(Long terminalEventTypeId) {
        this.terminalEventTypeId = terminalEventTypeId;
    }

    public String getEventDescription() {
        return eventDescription;
    }

    public void setEventDescription(String eventDescription) {
        this.eventDescription = eventDescription;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLinearG() {
        return linearG;
    }

    public void setLinearG(Double linearG) {
        this.linearG = linearG;
    }

    public Double getLateralG() {
        return lateralG;
    }

    public void setLateralG(Double lateralG) {
        this.lateralG = lateralG;
    }

    public Integer getAltitude() {
        return altitude;
    }

    public void setAltitude(Integer altitude) {
        this.altitude = altitude;
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

    public Integer getBearing() {
        return bearing;
    }

    public void setBearing(Integer bearing) {
        this.bearing = bearing;
    }

    public Boolean getIgnition() {
        return ignition;
    }

    public void setIgnition(Boolean ignition) {
        this.ignition = ignition;
    }

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    public Integer getRoadSpeed() {
        return roadSpeed;
    }

    public void setRoadSpeed(Integer roadSpeed) {
        this.roadSpeed = roadSpeed;
    }

    public Integer getRpm() {
        return rpm;
    }

    public void setRpm(Integer rpm) {
        this.rpm = rpm;
    }

    public Boolean getRoadSpeeding() {
        return roadSpeeding;
    }

    public void setRoadSpeeding(Boolean roadSpeeding) {
        this.roadSpeeding = roadSpeeding;
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

    public Integer getAnalog0() {
        return analog0;
    }

    public void setAnalog0(Integer analog0) {
        this.analog0 = analog0;
    }

    public Integer getAnalog1() {
        return analog1;
    }

    public void setAnalog1(Integer analog1) {
        this.analog1 = analog1;
    }

    public Integer getAnalog2() {
        return analog2;
    }

    public void setAnalog2(Integer analog2) {
        this.analog2 = analog2;
    }

    public Double getAdc0() {
        return adc0;
    }

    public void setAdc0(Double adc0) {
        this.adc0 = adc0;
    }

    public Double getAdc1() {
        return adc1;
    }

    public void setAdc1(Double adc1) {
        this.adc1 = adc1;
    }

    public Double getAdc2() {
        return adc2;
    }

    public void setAdc2(Double adc2) {
        this.adc2 = adc2;
    }

    public Long getPositionDescriptionId() {
        return positionDescriptionId;
    }

    public void setPositionDescriptionId(Long positionDescriptionId) {
        this.positionDescriptionId = positionDescriptionId;
    }

    public String getPositionDescription() {
        return positionDescription;
    }

    public void setPositionDescription(String positionDescription) {
        this.positionDescription = positionDescription;
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

    public Double getOutputState() {
        return outputState;
    }

    public void setOutputState(Double outputState) {
        this.outputState = outputState;
    }

    public Double getVext() {
        return vext;
    }

    public void setVext(Double vext) {
        this.vext = vext;
    }

    public Double getVgsm() {
        return vgsm;
    }

    public void setVgsm(Double vgsm) {
        this.vgsm = vgsm;
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

    public Integer getBatteryPercentageLeft() {
        return batteryPercentageLeft;
    }

    public void setBatteryPercentageLeft(Integer batteryPercentageLeft) {
        this.batteryPercentageLeft = batteryPercentageLeft;
    }

    public String getEventTs() {
        return eventTs;
    }

    public void setEventTs(String eventTs) {
        this.eventTs = eventTs;
    }

    public String getReceivedTs() {
        return receivedTs;
    }

    public void setReceivedTs(String receivedTs) {
        this.receivedTs = receivedTs;
    }

    public Double getZaccel() {
        return zaccel;
    }

    public void setZaccel(Double zaccel) {
        this.zaccel = zaccel;
    }

    public Double getYaccel() {
        return yaccel;
    }

    public void setYaccel(Double yaccel) {
        this.yaccel = yaccel;
    }

    public Double getXaccel() {
        return xaccel;
    }

    public void setXaccel(Double xaccel) {
        this.xaccel = xaccel;
    }

    public Integer getGpsFixType() {
        return gpsFixType;
    }

    public void setGpsFixType(Integer gpsFixType) {
        this.gpsFixType = gpsFixType;
    }

    public Long getTerminalId() {
        return terminalId;
    }

    public void setTerminalId(Long terminalId) {
        this.terminalId = terminalId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }
}
