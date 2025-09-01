package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents a vehicle with identification details from UD Trucks data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Vehicle {

    private String vin;

    private String chassisId;

    public Vehicle() {

    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public String getChassisId() {
        return chassisId;
    }

    public void setChassisId(String chassisId) {
        this.chassisId = chassisId;
    }
}
