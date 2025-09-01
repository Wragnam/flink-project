package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents driving behavior data from UD Trucks, including position and additional telemetry data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UdDrivingBehaviour implements Serializable {

    private String triggerType;

    private String triggerTime;

    private String receivedTime;

    private Position position;

    private AdditionalData drivingBehavioursAdditionalData;

    public static class AdditionalData{
        private AdditionalDataItem fuelLevel;

        private AdditionalDataItem totalDistance;

        private AdditionalDataItem totalEngineTime;

        public AdditionalDataItem getFuelLevel() {
            return fuelLevel;
        }

        public void setFuelLevel(AdditionalDataItem fuelLevel) {
            this.fuelLevel = fuelLevel;
        }

        public AdditionalDataItem getTotalDistance() {
            return totalDistance;
        }

        public void setTotalDistance(AdditionalDataItem totalDistance) {
            this.totalDistance = totalDistance;
        }

        public AdditionalDataItem getTotalEngineTime() {
            return totalEngineTime;
        }

        public void setTotalEngineTime(AdditionalDataItem totalEngineTime) {
            this.totalEngineTime = totalEngineTime;
        }
    }

    public String getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(String triggerType) {
        this.triggerType = triggerType;
    }

    public String getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(String triggerTime) {
        this.triggerTime = triggerTime;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public AdditionalData getDrivingBehavioursAdditionalData() {
        return drivingBehavioursAdditionalData;
    }

    public void setDrivingBehavioursAdditionalData(AdditionalData drivingBehavioursAdditionalData) {
        this.drivingBehavioursAdditionalData = drivingBehavioursAdditionalData;
    }
}
