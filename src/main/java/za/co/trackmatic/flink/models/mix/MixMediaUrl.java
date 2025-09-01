package za.co.trackmatic.flink.models.mix;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents media URLs related to a Mix event, including different camera perspectives.
 * This class supports JSON deserialization with unknown properties ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MixMediaUrl implements Serializable {

    public String Road;

    @JsonProperty("In Cab")
    public String inCab;

    public String Driver;

    public String getRoad() {
        return Road;
    }

    public void setRoad(String road) {
        Road = road;
    }

    public String getInCab() {
        return inCab;
    }

    public void setInCab(String inCab) {
        this.inCab = inCab;
    }

    public String getDriver() {
        return Driver;
    }

    public void setDriver(String driver) {
        Driver = driver;
    }
}
