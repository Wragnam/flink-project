package za.co.trackmatic.flink.models.trackmatic;

import java.io.Serializable;

/**
 * Represents a stop event with start time, end time, and duration.
 */
public class StopEvent implements Serializable {

    @Override
    public String toString() {
        return "StopEvent{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", duration=" + duration +
                '}';
    }

    public StopEvent(){

    }

    /**
     * Start time of the stop event.
     */
    private long startTime;

    /**
     * End time of the stop event.
     */
    private long endTime;

    /**
     * Duration of the stop event.
     */
    private long duration;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}
