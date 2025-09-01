package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the routing violations configuration for stops.
 * Contains nested RoutingViolation details for different stop types.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoutingViolations implements Serializable {

    /**
     * Represents a routing violation with a duration in seconds.
     */
    public static class RoutingViolation{

        /**
         * Duration of the violation in seconds.
         */
        private Integer durationInSeconds;

        public Integer getDurationInSeconds() {
            return durationInSeconds;
        }

        public void setDurationInSeconds(Integer durationInSeconds) {
            this.durationInSeconds = durationInSeconds;
        }
    }

    /**
     * Configuration for unplanned stop routing violations.
     */
    private RoutingViolation unplannedStop;

    /**
     * Configuration for excess stop routing violations.
     */
    private RoutingViolation excessStop;

    /**
     * Configuration for unknown stop routing violations.
     */
    private RoutingViolation unknownStop;

    public RoutingViolation getUnplannedStop() {
        return unplannedStop;
    }

    public void setUnplannedStop(RoutingViolation unplannedStop) {
        this.unplannedStop = unplannedStop;
    }

    public RoutingViolation getExcessStop() {
        return excessStop;
    }

    public void setExcessStop(RoutingViolation excessStop) {
        this.excessStop = excessStop;
    }

    public RoutingViolation getUnknownStop() {
        return unknownStop;
    }

    public void setUnknownStop(RoutingViolation unknownStop) {
        this.unknownStop = unknownStop;
    }
}
