package za.co.trackmatic.flink.models.mappingAPI;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;

/**
 * Request model for retrieving speed limit information at a specific geographic location.
 *
 * <p>Extends {@link LatLong} to provide latitude and longitude, with an optional
 * source parameter indicating the data source for the speed limit information.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SpeedLimitRequest extends LatLong implements Serializable {

    public SpeedLimitRequest(LatLong latLong){
        this.setLat(latLong.getLat());
        this.setLng(latLong.getLng());
    }

    private String source = "tps";

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
