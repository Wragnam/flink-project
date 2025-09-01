package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents fuel-related data for a vehicle in the Cartrack system.
 * Contains information about the fuel level, percentage left, total consumed, and the timestamp of the last update.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackFuel implements Serializable {

    private String updated;

    private Integer level;

    @JsonProperty("percentage_left")
    private Integer percentageLeft;

    @JsonProperty("total_consumed")
    private Float totalConsumed;

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(String updated) {
        this.updated = updated;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Integer getPercentageLeft() {
        return percentageLeft;
    }

    public void setPercentageLeft(Integer percentageLeft) {
        this.percentageLeft = percentageLeft;
    }

    public Float getTotalConsumed() {
        return totalConsumed;
    }

    public void setTotalConsumed(Float totalConsumed) {
        this.totalConsumed = totalConsumed;
    }
}
