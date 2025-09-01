package za.co.trackmatic.flink.events.surfsight;

import za.co.trackmatic.flink.models.surfsight.SurfsightCameraFiles;

import java.io.Serializable;
import java.util.List;

/**
 * Represents specific Surfsight event data containing camera files.
 */
public class SurfsightSpecificData implements Serializable {

    /**
     * The list of Surfsight camera files associated with the event.
     */
    private List<SurfsightCameraFiles> cameraFiles;

    public List<SurfsightCameraFiles> getCameraFiles() {
        return cameraFiles;
    }

    public void setCameraFiles(List<SurfsightCameraFiles> files) {
        this.cameraFiles = files;
    }
}
