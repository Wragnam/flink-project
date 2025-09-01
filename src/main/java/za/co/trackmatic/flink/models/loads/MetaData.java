package za.co.trackmatic.flink.models.loads;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Represents metadata information related to a load or event,
 * including app details, serial identifier, synchronization tags, and type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaData {

    private String app;

    private String serial;

    private List<String> sync;

    private String type;

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
}
