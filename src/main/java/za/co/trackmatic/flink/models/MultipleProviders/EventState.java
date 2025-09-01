package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.models.trackmatic.StopEvent;

import java.io.Serializable;
import java.util.*;

/**
 * Holds the current state of various vehicle-related events, including panic,
 * battery events, stop events, door states, geofence arrivals, and more.
 * <p>
 * Used to track and update the ongoing status of events related to a vehicle during processing.
 */
public class EventState implements Serializable {

    /**
     * Constructs a new EventState with default initial values.
     */
    public EventState() {
        this.panic = 0;
        this.mainBatteryEventCounter = 0;
        this.backupBatteryEventCounter = 0;
        this.unknownStopStatus = 0;
        this.accidentCounter = 0;
        this.lastPanicTime = 0;
        this.lastTime = 0;
        this.speedIgnitionCounter = 0;
        this.unknownStopEvent = new StopEvent();
        this.unplannedStopEvent = new StopEvent();
        this.geofences = new HashMap<>();
        this.currentArrivedAtGeofences = new HashSet<>();
        this.setUnplannedStop(false);
    }

    /**
     * Timestamp of the last event processed.
     */
    private long lastTime;

    /**
     * Map of geofence IDs to status or related string data.
     */
    private Map<String, String> geofences;

    /**
     * True if the vehicle ignition is currently on.
     */
    private Boolean ignitionOn;

    /**
     * True if the vehicle is currently in an unplanned stop state.
     */
    private boolean unplannedStop;

    /**
     * Timestamp of the last panic event.
     */
    private long lastPanicTime;

    /**
     * Counter for main battery-related events.
     */
    private Integer mainBatteryEventCounter;

    /**
     * Counter for backup battery-related events.
     */
    private Integer backupBatteryEventCounter;

    /**
     * Counter for panic events.
     * Ensuring there are not multiple panics created for one event
     * if the asset is in a state of panic
     */
    private Integer panic;

    /**
     * Identifier of the current unplanned stop event, if any.
     */
    private String unplannedStopId;

    /**
     * Counter for accident-related events.
     * Ensuring minimum events if the vehicle is in a
     * state of accident
     */
    private Integer accidentCounter;

    /** StopEvent representing an unknown stop condition. */
    private StopEvent unknownStopEvent;

    /** StopEvent representing an unplanned stop condition. */
    private StopEvent unplannedStopEvent;

    /** Status code representing unknown stop status. */
    private Integer unknownStopStatus;

    /** Set of geofence IDs where the vehicle is currently arrived. */
    private Set<String> currentArrivedAtGeofences;

    /** Whether the rear door is currently open. */
    private Boolean rearDoorOpen;

    /** Whether the side door is currently open. */
    private Boolean sideDoorOpen;

    /** Whether zone 1 door is currently open. */
    private Boolean zone1DoorOpen;

    /** Whether zone 2 door is currently open. */
    private Boolean zone2DoorOpen;

    /** Whether zone 3 door is currently open. */
    private Boolean zone3DoorOpen;

    /** Whether the fuel level is low. */
    private boolean fuelLevelLow;

    /** Set point 1 for an unspecified measurement or threshold. */
    private Double setPoint1;

    /** Set point 2 for an unspecified measurement or threshold. */
    private Double setPoint2;

    /** Set point 3 for an unspecified measurement or threshold. */
    private Double setPoint3;

    /** Counter for speed and ignition related events.
     * Used when there is no other indication of ignition
     * on a device.
     */
    private int speedIgnitionCounter;

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public Map<String, String> getGeofences() {
        return geofences;
    }

    public void setGeofences(Map<String, String> geofences) {
        this.geofences = geofences;
    }

    public Boolean isIgnitionOn() {
        return ignitionOn;
    }

    public void setIgnitionOn(Boolean ignitionOn) {
        this.ignitionOn = ignitionOn;
    }

    public boolean isUnplannedStop() {
        return unplannedStop;
    }

    public void setUnplannedStop(boolean unplannedStop) {
        this.unplannedStop = unplannedStop;
    }

    public String getUnplannedStopId() {
        return unplannedStopId;
    }

    public void setUnplannedStopId(String unplannedStopId) {
        this.unplannedStopId = unplannedStopId;
    }

    public StopEvent getUnknownStopEvent() {
        return unknownStopEvent;
    }

    public void setUnknownStopEvent(StopEvent unknownStopEvent) {
        this.unknownStopEvent = unknownStopEvent;
    }

    public StopEvent getUnplannedStopEvent() {
        return unplannedStopEvent;
    }

    public void setUnplannedStopEvent(StopEvent unplannedStopEvent) {
        this.unplannedStopEvent = unplannedStopEvent;
    }

    public Integer getUnknownStopStatus() {
        return unknownStopStatus;
    }

    public void setUnknownStopStatus(Integer unknownStopStatus) {
        this.unknownStopStatus = unknownStopStatus;
    }

    public Set<String> getCurrentArrivedAtGeofences() {
        return currentArrivedAtGeofences;
    }

    public void setCurrentArrivedAtGeofences(Set<String> currentArrivedAtGeofences) {
        this.currentArrivedAtGeofences = currentArrivedAtGeofences;
    }

    public long getLastPanicTime() {
        return lastPanicTime;
    }

    public void setLastPanicTime(long lastPanicTime) {
        this.lastPanicTime = lastPanicTime;
    }

    public Integer getPanic() {
        return panic;
    }

    public void setPanic(Integer panic) {
        this.panic = panic;
    }

    public Integer getMainBatteryEventCounter() {
        return mainBatteryEventCounter;
    }

    public void setMainBatteryEventCounter(Integer mainBatteryEventCounter) {
        this.mainBatteryEventCounter = mainBatteryEventCounter;
    }

    public Integer getBackupBatteryEventCounter() {
        return backupBatteryEventCounter;
    }

    public void setBackupBatteryEventCounter(Integer backupBatteryEventCounter) {
        this.backupBatteryEventCounter = backupBatteryEventCounter;
    }

    public Integer getAccidentCounter() {
        return accidentCounter;
    }

    public void setAccidentCounter(Integer accidentCounter) {
        this.accidentCounter = accidentCounter;
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

    public boolean isFuelLevelLow() {
        return fuelLevelLow;
    }

    public void setFuelLevelLow(boolean fuelLevelLow) {
        this.fuelLevelLow = fuelLevelLow;
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

    public int getSpeedIgnitionCounter() {
        return speedIgnitionCounter;
    }

    public void setSpeedIgnitionCounter(int speedIgnitionCounter) {
        this.speedIgnitionCounter = speedIgnitionCounter;
    }

    public void incrementSpeedIgnitionCounter() {
        this.speedIgnitionCounter += 1;
    }

    public Boolean getZone1DoorOpen() {
        return zone1DoorOpen;
    }

    public void setZone1DoorOpen(Boolean zone1DoorOpen) {
        this.zone1DoorOpen = zone1DoorOpen;
    }

    public Boolean getZone2DoorOpen() {
        return zone2DoorOpen;
    }

    public void setZone2DoorOpen(Boolean zone2DoorOpen) {
        this.zone2DoorOpen = zone2DoorOpen;
    }

    public Boolean getZone3DoorOpen() {
        return zone3DoorOpen;
    }

    public void setZone3DoorOpen(Boolean zone3DoorOpen) {
        this.zone3DoorOpen = zone3DoorOpen;
    }
}
