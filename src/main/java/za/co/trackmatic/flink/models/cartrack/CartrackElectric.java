package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents electric-related status information for a Cartrack device.
 * <p>
 * Contains details about battery percentage left and the timestamp of the battery status.
 * JSON properties are mapped to Java fields for serialization/deserialization.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackElectric implements Serializable {

    @JsonProperty("battery_percentage_left")
    private Integer batteryPercentageLeft;

    @JsonProperty("battery_ts")
    private String batteryTs;

    public Integer getBatteryPercentageLeft() {
        return batteryPercentageLeft;
    }

    public void setBatteryPercentageLeft(Integer batteryPercentageLeft) {
        this.batteryPercentageLeft = batteryPercentageLeft;
    }

    public String getBatteryTs() {
        return batteryTs;
    }

    public void setBatteryTs(String batteryTs) {
        this.batteryTs = batteryTs;
    }
}
