package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents grouped trigger information related to charging for Volvo vehicles.
 * Contains charging status and charging connection status details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoGroupTrigger implements Serializable {
    private VolvoChargingInfo chargingStatusInfo;

    private VolvoChargingInfo chargingConnectionStatusInfo;

    public VolvoChargingInfo getChargingStatusInfo() {
        return chargingStatusInfo;
    }

    public void setChargingStatusInfo(VolvoChargingInfo chargingStatusInfo) {
        this.chargingStatusInfo = chargingStatusInfo;
    }

    public VolvoChargingInfo getChargingConnectionStatusInfo() {
        return chargingConnectionStatusInfo;
    }

    public void setChargingConnectionStatusInfo(VolvoChargingInfo chargingConnectionStatusInfo) {
        this.chargingConnectionStatusInfo = chargingConnectionStatusInfo;
    }
}
