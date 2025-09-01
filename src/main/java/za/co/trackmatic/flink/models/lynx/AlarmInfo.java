package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents alarm-related information such as response status,
 * alarm code, and description.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlarmInfo implements Serializable {
    private String response;
    private String code;
    private String description;

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
