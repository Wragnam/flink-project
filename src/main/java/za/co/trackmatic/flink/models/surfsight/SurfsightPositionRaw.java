package za.co.trackmatic.flink.models.surfsight;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a raw position data object from Surfsight devices.
 * Extends the base data model with additional accuracy information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SurfsightPositionRaw extends DataBase implements Serializable  {

    private double accuracy;

    private TmMetadata meta;

    public SurfsightPositionRaw() {

    }

    public double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }

    @Override
    public TmMetadata getMeta() {
        return meta;
    }

    @Override
    public void setMeta(TmMetadata meta) {
        this.meta = meta;
    }
}
