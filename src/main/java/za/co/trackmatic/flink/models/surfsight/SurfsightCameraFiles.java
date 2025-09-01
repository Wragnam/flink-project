package za.co.trackmatic.flink.models.surfsight;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a camera file linked to a Surfsight event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SurfsightCameraFiles implements Serializable {
    private long cameraId;

    private long file;

    private String fileType;

    public SurfsightCameraFiles() {

    }

    public long getCameraId() {
        return cameraId;
    }

    public void setCameraId(long cameraId) {
        this.cameraId = cameraId;
    }

    public long getFile() {
        return file;
    }

    public void setFile(long file) {
        this.file = file;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }
}
