package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents an additional data item containing a numeric value and its units.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdditionalDataItem implements Serializable {

    /** Numeric value of the data item. */
    private double value;

    /** Units corresponding to the value, e.g., "km/h", "°C". */
    private String units;

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }
}
