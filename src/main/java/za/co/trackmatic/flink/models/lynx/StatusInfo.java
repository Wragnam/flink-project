package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents detailed status information for a Lynx asset, including
 * electrical metrics, engine details, door statuses, and energy usage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatusInfo implements Serializable {
    private String powerStatus;
    private Double switchOnHours;
    private Double engineRunHours;
    private Double batteryVoltage;
    private Double batteryCurrent;
    private Boolean rearDoorOpen;
    private Boolean sideDoorOpen;
    private Double fuelLevel;
    private String truStatus;
    private String comp1OperatingMode;
    private String comp2OperatingMode;
    private String comp3OperatingMode;
    private Double positionSpeed;
    private Double vehicleMileage;
    private Double standByHours;
    private Boolean movementStatus;
    private String enginePowerMode;
    private String engineControlMode;
    private String engineSpeed;
    private Boolean switchKeyStatus;
    private Double roadEnergy;
    private Double standByEnergy;
    private Double powerMode;

    public String getPowerStatus() {
        return powerStatus;
    }

    public void setPowerStatus(String powerStatus) {
        this.powerStatus = powerStatus;
    }

    public Double getSwitchOnHours() {
        return switchOnHours;
    }

    public void setSwitchOnHours(Double switchOnHours) {
        this.switchOnHours = switchOnHours;
    }

    public Double getEngineRunHours() {
        return engineRunHours;
    }

    public void setEngineRunHours(Double engineRunHours) {
        this.engineRunHours = engineRunHours;
    }

    public Double getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(Double batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public Double getBatteryCurrent() {
        return batteryCurrent;
    }

    public void setBatteryCurrent(Double batteryCurrent) {
        this.batteryCurrent = batteryCurrent;
    }

    public Boolean getRearDoorOpen() {
        return rearDoorOpen;
    }

    public void setRearDoorOpen(Boolean rearDoorOpen) {
        this.rearDoorOpen = rearDoorOpen;
    }

    public Boolean getSideDoorOpen() {
        return sideDoorOpen;
    }

    public void setSideDoorOpen(Boolean sideDoorOpen) {
        this.sideDoorOpen = sideDoorOpen;
    }

    public Double getFuelLevel() {
        return fuelLevel;
    }

    public void setFuelLevel(Double fuelLevel) {
        this.fuelLevel = fuelLevel;
    }

    public String getTruStatus() {
        return truStatus;
    }

    public void setTruStatus(String truStatus) {
        this.truStatus = truStatus;
    }

    public String getComp1OperatingMode() {
        return comp1OperatingMode;
    }

    public void setComp1OperatingMode(String comp1OperatingMode) {
        this.comp1OperatingMode = comp1OperatingMode;
    }

    public String getComp2OperatingMode() {
        return comp2OperatingMode;
    }

    public void setComp2OperatingMode(String comp2OperatingMode) {
        this.comp2OperatingMode = comp2OperatingMode;
    }

    public String getComp3OperatingMode() {
        return comp3OperatingMode;
    }

    public void setComp3OperatingMode(String comp3OperatingMode) {
        this.comp3OperatingMode = comp3OperatingMode;
    }

    public Double getPositionSpeed() {
        return positionSpeed;
    }

    public void setPositionSpeed(Double positionSpeed) {
        this.positionSpeed = positionSpeed;
    }

    public Double getVehicleMileage() {
        return vehicleMileage;
    }

    public void setVehicleMileage(Double vehicleMileage) {
        this.vehicleMileage = vehicleMileage;
    }

    public Double getStandByHours() {
        return standByHours;
    }

    public void setStandByHours(Double standByHours) {
        this.standByHours = standByHours;
    }

    public Boolean getMovementStatus() {
        return movementStatus;
    }

    public void setMovementStatus(Boolean movementStatus) {
        this.movementStatus = movementStatus;
    }

    public String getEnginePowerMode() {
        return enginePowerMode;
    }

    public void setEnginePowerMode(String enginePowerMode) {
        this.enginePowerMode = enginePowerMode;
    }

    public String getEngineControlMode() {
        return engineControlMode;
    }

    public void setEngineControlMode(String engineControlMode) {
        this.engineControlMode = engineControlMode;
    }

    public String getEngineSpeed() {
        return engineSpeed;
    }

    public void setEngineSpeed(String engineSpeed) {
        this.engineSpeed = engineSpeed;
    }

    public Boolean getSwitchKeyStatus() {
        return switchKeyStatus;
    }

    public void setSwitchKeyStatus(Boolean switchKeyStatus) {
        this.switchKeyStatus = switchKeyStatus;
    }

    public Double getRoadEnergy() {
        return roadEnergy;
    }

    public void setRoadEnergy(Double roadEnergy) {
        this.roadEnergy = roadEnergy;
    }

    public Double getStandByEnergy() {
        return standByEnergy;
    }

    public void setStandByEnergy(Double standByEnergy) {
        this.standByEnergy = standByEnergy;
    }

    public Double getPowerMode() {
        return powerMode;
    }

    public void setPowerMode(Double powerMode) {
        this.powerMode = powerMode;
    }
}
