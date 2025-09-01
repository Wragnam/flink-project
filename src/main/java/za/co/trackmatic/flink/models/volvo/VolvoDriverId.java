package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents driver identification information for Volvo vehicles.
 * Contains both tachograph and OEM driver IDs.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoDriverId implements Serializable {
    private VolvoTachoDriverId tachoDriverIdentification;

    private VolvoOemDriverId oemDriverIdentification;

    public VolvoTachoDriverId getTachoDriverIdentification() {
        return tachoDriverIdentification;
    }

    public void setTachoDriverIdentification(VolvoTachoDriverId tachoDriverIdentification) {
        this.tachoDriverIdentification = tachoDriverIdentification;
    }

    public VolvoOemDriverId getOemDriverIdentification() {
        return oemDriverIdentification;
    }

    public void setOemDriverIdentification(VolvoOemDriverId oemDriverIdentification) {
        this.oemDriverIdentification = oemDriverIdentification;
    }
}
