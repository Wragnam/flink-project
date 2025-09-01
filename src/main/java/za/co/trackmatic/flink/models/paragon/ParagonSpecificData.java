package za.co.trackmatic.flink.models.paragon;

import za.co.trackmatic.flink.events.paragon.AccidentEvent;
import za.co.trackmatic.flink.events.universal.BatteryEvent;
import za.co.trackmatic.flink.models.trackmatic.StopEvent;
import za.co.trackmatic.flink.models.trackmatic.GeofenceEvent;

import java.io.Serializable;

public class ParagonSpecificData implements Serializable {

    public ParagonSpecificData(){}
    private String date;
    private double direction;

    private double speed;

    private String ignition;

    private double accuracy;

    private Double dischargeAir;

    private Double returnAir;

    private BatteryEvent batteryEvent;

    private GeofenceEvent geofenceEvent;

    private StopEvent unknownStopEvent;

    private AccidentEvent accidentEvent;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getDirection() {
        return direction;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getIgnition() {
        return ignition;
    }

    public void setIgnition(String ignition) {
        this.ignition = ignition;
    }

    public double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    public GeofenceEvent getGeofenceEvent() {
        return geofenceEvent;
    }

    public void setGeofenceEvent(GeofenceEvent geofenceEvent) {
        this.geofenceEvent = geofenceEvent;
    }

    public BatteryEvent getBatteryEvent() {
        return batteryEvent;
    }

    public void setBatteryEvent(BatteryEvent batteryEvent) {
        this.batteryEvent = batteryEvent;
    }

    public StopEvent getUnknownStopEvent() {
        return unknownStopEvent;
    }

    public void setUnknownStopEvent(StopEvent stopEvent) {
        this.unknownStopEvent = stopEvent;
    }

    public AccidentEvent getAccidentEvent() {
        return accidentEvent;
    }

    public void setAccidentEvent(AccidentEvent accidentEvent) {
        this.accidentEvent = accidentEvent;
    }

    public Double getDischargeAir() {
        return dischargeAir;
    }

    public void setDischargeAir(Double dischargeAir) {
        this.dischargeAir = dischargeAir;
    }

    public Double getReturnAir() {
        return returnAir;
    }

    public void setReturnAir(Double returnAir) {
        this.returnAir = returnAir;
    }
}
