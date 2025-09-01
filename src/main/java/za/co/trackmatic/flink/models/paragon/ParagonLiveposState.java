package za.co.trackmatic.flink.models.paragon;

import java.io.Serializable;

/**
 * Represents the live position status of a Paragon device.
 * Stores the last known status and the timestamp of when it changed.
 */
public class ParagonLiveposState implements Serializable {

    /** Last known status of the Paragon device (e.g., MOVING, STOPPED, IDLE). */
    private String lastStatus;

    /** ISO 8601 formatted timestamp indicating when the last status change occurred. */
    private String lastStatusChangedDate;

    public String getLastStatus() {
        return lastStatus;
    }

    public void setLastStatus(String lastStatus) {
        this.lastStatus = lastStatus;
    }

    public String getLastStatusChangedDate() {
        return lastStatusChangedDate;
    }

    public void setLastStatusChangedDate(String lastStatusChangedDate) {
        this.lastStatusChangedDate = lastStatusChangedDate;
    }
}
