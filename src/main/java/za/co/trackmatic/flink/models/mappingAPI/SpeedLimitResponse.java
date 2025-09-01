package za.co.trackmatic.flink.models.mappingAPI;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the response for a speed limit query at a specific location.
 *
 * <p>Contains information about whether the speed limit is an estimated value,
 * and the maximum allowed speed.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SpeedLimitResponse implements Serializable {

    private boolean estimated;

    private double maxSpeed;

    public boolean isEstimated() {
        return estimated;
    }

    public void setEstimated(boolean estimated) {
        this.estimated = estimated;
    }

    public double getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(double maxSpeed) {
        this.maxSpeed = maxSpeed;
    }
}
