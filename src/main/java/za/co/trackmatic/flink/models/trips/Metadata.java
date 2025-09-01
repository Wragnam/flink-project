package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;
import java.util.List;

/**
 * Represents metadata related to a trip or tracking event, including application info,
 * device serial number, synchronization info, and software versioning.
 */
public class Metadata implements Serializable {

    public Metadata(){

    }

    /**
     * Name of the application that generated the data.
     */
    private String app;

    /**
     * Serial number of the device or source generating the data.
     */
    private String serial;

    /**
     * List of synchronization markers or identifiers.
     */
    private List<String> sync;

    /**
     * Type/category of the metadata.
     */
    private String type;

    /**
     * Version of the application or system that generated the data.
     */
    private String version;

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public List<String> getSync() {
        return sync;
    }

    public void setSync(List<String> sync) {
        this.sync = sync;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
