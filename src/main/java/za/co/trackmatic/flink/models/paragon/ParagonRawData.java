package za.co.trackmatic.flink.models.paragon;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents raw data received from a Paragon tracking device.
 * This includes the device type, serial number, firmware version, the list of data items,
 * and any associated Trackmatic metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ParagonRawData implements Serializable {

    private String type;

    private String serial;

    private String sver;

    private List<DataItem> data;

    private TmMetadata metadata;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public String getSver() {
        return sver;
    }

    public void setSver(String sver) {
        this.sver = sver;
    }

    public List<DataItem> getData() {
        return data;
    }

    public void setData(List<DataItem> data) {
        this.data = data;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
