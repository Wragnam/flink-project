package za.co.trackmatic.flink.models.loads.extrapolation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.loads.MetaData;

/**
 * Represents an extrapolation request and response event containing metadata and next stop information payload.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExtrapolationReqAndRespEvent {

    private MetaData meta;

    private NextStopInformation payload;

    public MetaData getMeta() {
        return meta;
    }

    public void setMeta(MetaData meta) {
        this.meta = meta;
    }

    public NextStopInformation getPayload() {
        return payload;
    }

    public void setPayload(NextStopInformation payload) {
        this.payload = payload;
    }
}
