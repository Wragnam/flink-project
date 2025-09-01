package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a trigger event in Volvo vehicle data, including driver info, tell-tale status, and other related triggers.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoTrigger implements Serializable {
    private String triggerType;

    private String context;

    private List<String> triggerInfo;

    private VolvoDriverId driverId;

    private String ptoId;

    private VolvoTellTaleInfo tellTaleInfo;

    private VolvoGroupTrigger volvoGroupTrigger;

    public String getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(String triggerType) {
        this.triggerType = triggerType;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public List<String> getTriggerInfo() {
        return triggerInfo;
    }

    public void setTriggerInfo(List<String> triggerInfo) {
        this.triggerInfo = triggerInfo;
    }

    public VolvoDriverId getDriverId() {
        return driverId;
    }

    public void setDriverId(VolvoDriverId driverId) {
        this.driverId = driverId;
    }

    public String getPtoId() {
        return ptoId;
    }

    public void setPtoId(String ptoId) {
        this.ptoId = ptoId;
    }

    public VolvoTellTaleInfo getTellTaleInfo() {
        return tellTaleInfo;
    }

    public void setTellTaleInfo(VolvoTellTaleInfo tellTaleInfo) {
        this.tellTaleInfo = tellTaleInfo;
    }

    public VolvoGroupTrigger getVolvoGroupTrigger() {
        return volvoGroupTrigger;
    }

    public void setVolvoGroupTrigger(VolvoGroupTrigger volvoGroupTrigger) {
        this.volvoGroupTrigger = volvoGroupTrigger;
    }
}
