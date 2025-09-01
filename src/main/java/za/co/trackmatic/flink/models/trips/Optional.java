package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents optional GPS-related information for a trip or position.
 * Typically used to store additional telemetry such as altitude and satellite count.
 */
public class Optional implements Serializable {

    /**
     * The altitude of the position in meters.
     */
    private double alti;

    /**
     * The number of satellites in view at the time of the reading.
     */
    private Integer sat;

    public double getAlti() {
        return alti;
    }

    public void setAlti(double alti) {
        this.alti = alti;
    }

    public Integer getSat() {
        return sat;
    }

    public void setSat(Integer sat) {
        this.sat = sat;
    }
}
