package za.co.trackmatic.flink.models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a simple key-value pair.
 * Used for configurations or properties with string keys and values.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyValuePair implements Serializable {

    /**
     * The key in the key-value pair.
     */
    private String k;

    /**
     * The value in the key-value pair.
     */
    private String v;

    public KeyValuePair() {

    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }
}
