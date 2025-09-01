package za.co.trackmatic.flink.models.datacache;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;

import java.util.List;

/**
 * Represents the response for a geofence containment query.
 * Indicates whether a point is inside any geofence and provides details about the geofence.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PointInGeofenceResponse {

    private boolean inGeofence;

    private GeofenceItem geofence;

    private List<GeofenceItem> geofences;

    public PointInGeofenceResponse() {

    }

    public boolean isInGeofence() {
        return inGeofence;
    }

    public void setInGeofence(boolean inGeofence) {
        this.inGeofence = inGeofence;
    }

    public GeofenceItem getGeofence() {
        return geofence;
    }

    public void setGeofence(GeofenceItem geofence) {
        this.geofence = geofence;
    }

    public List<GeofenceItem> getGeofences() {
        return geofences;
    }

    public void setGeofences(List<GeofenceItem> geofences) {
        this.geofences = geofences;
    }
}
