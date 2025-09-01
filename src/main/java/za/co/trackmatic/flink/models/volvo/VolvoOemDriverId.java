package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the OEM (Original Equipment Manufacturer) driver identification information
 * provided by Volvo.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoOemDriverId implements Serializable {
    private String idType;

    private String oemDriverIdentification;

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public String getOemDriverIdentification() {
        return oemDriverIdentification;
    }

    public void setOemDriverIdentification(String oemDriverIdentification) {
        this.oemDriverIdentification = oemDriverIdentification;
    }
}
