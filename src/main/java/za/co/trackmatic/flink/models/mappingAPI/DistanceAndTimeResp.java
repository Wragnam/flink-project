package za.co.trackmatic.flink.models.mappingAPI;

import java.io.Serializable;

/**
 * Response model containing distance and time information.
 *
 * <p>This class holds the calculated distance (in metres) and estimated time (in seconds)
 * between two points.</p>
 */
public class DistanceAndTimeResp implements Serializable {

    @Override
    public String toString() {
        return "DistanceAndTimeResp{" +
                "distanceInMetres=" + distanceInMetres +
                ", timeInSeconds=" + timeInSeconds +
                '}';
    }

    private long distanceInMetres;

    private long timeInSeconds;

    public long getDistanceInMetres() {
        return distanceInMetres;
    }

    public void setDistanceInMetres(long distanceInMetres) {
        this.distanceInMetres = distanceInMetres;
    }

    public long getTimeInSeconds() {
        return timeInSeconds;
    }

    public void setTimeInSeconds(long timeInSeconds) {
        this.timeInSeconds = timeInSeconds;
    }
}
