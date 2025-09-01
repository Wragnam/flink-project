package za.co.trackmatic.flink.models.scania;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the trigger metadata that indicates why a specific Scania message was generated.
 * This can include information such as the type of trigger (e.g., ignition on, periodic update)
 * and additional context (e.g., user-defined or system-defined).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TriggerType implements Serializable {
    private String triggerType;

    private String context;

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
}
