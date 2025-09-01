package za.co.trackmatic.flink.models.mappingAPI;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;

/**
 * Request body model for distance and time calculations between two geographical points.
 *
 * <p>Contains start and end coordinates, configuration options for the mapping request,
 * and a source identifier string.</p>
 *
 * <p>Unknown JSON properties are ignored during deserialization.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DistanceAndTimeReqBody implements Serializable {

    public DistanceAndTimeReqBody(){

        this.config = new MappingConfig();
    }

    private LatLong start;

    private LatLong end;

    private MappingConfig config;

    private String source = "tps";

    public LatLong getStart() {
        return start;
    }

    public void setStart(LatLong start) {
        this.start = start;
    }

    public LatLong getEnd() {
        return end;
    }

    public void setEnd(LatLong end) {
        this.end = end;
    }

    public MappingConfig getConfig() {
        return config;
    }

    public void setConfig(MappingConfig config) {
        this.config = config;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
