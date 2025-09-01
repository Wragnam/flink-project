package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents information about a connected truck, including its identity,
 * license plate, signal strength, and MAC identifier.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectedTruckInfo implements Serializable {
    private String truckName;
    private String licensePlateNumber;
    private Double signalStrength;
    private String macId;

    public String getTruckName() {
        return truckName;
    }

    public void setTruckName(String truckName) {
        this.truckName = truckName;
    }

    public String getLicensePlateNumber() {
        return licensePlateNumber;
    }

    public void setLicensePlateNumber(String licensePlateNumber) {
        this.licensePlateNumber = licensePlateNumber;
    }

    public Double getSignalStrength() {
        return signalStrength;
    }

    public void setSignalStrength(Double signalStrength) {
        this.signalStrength = signalStrength;
    }

    public String getMacId() {
        return macId;
    }

    public void setMacId(String macId) {
        this.macId = macId;
    }
}
