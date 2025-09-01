package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a Volvo vehicle tell-tale indicator information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoTellTaleInfo implements Serializable {
    private String tellTale;
    private String oemTellTale;
    private String state;

    public String getTellTale() {
        return tellTale;
    }

    public void setTellTale(String tellTale) {
        this.tellTale = tellTale;
    }

    public String getOemTellTale() {
        return oemTellTale;
    }

    public void setOemTellTale(String oemTellTale) {
        this.oemTellTale = oemTellTale;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
