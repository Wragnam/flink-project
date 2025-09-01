package za.co.trackmatic.flink.events.paragon;

import java.io.Serializable;

/**
 * Represents an accident event with accelerometer data from the Paragon system.
 */
public class AccidentEvent implements Serializable {
    public AccidentEvent(){}

    /**
     * The timestamp of the event in seconds since epoch.
     */
    private long time;

    /**
     * The X-axis accelerometer reading.
     */
    private double accelerometerX;

    /**
     * The Y-axis accelerometer reading.
     */
    private double accelerometerY;

    /**
     * The Z-axis accelerometer reading.
     */
    private double accelerometerZ;


    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getAccelerometerX() {
        return accelerometerX;
    }

    public void setAccelerometerX(double accelerometerX) {
        this.accelerometerX = accelerometerX;
    }

    public double getAccelerometerY() {
        return accelerometerY;
    }

    public void setAccelerometerY(double accelerometerY) {
        this.accelerometerY = accelerometerY;
    }

    public double getAccelerometerZ() {
        return accelerometerZ;
    }

    public void setAccelerometerZ(double accelerometerZ) {
        this.accelerometerZ = accelerometerZ;
    }
}
