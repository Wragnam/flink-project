package za.co.trackmatic.flink.models.blackberry;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a raw event from a Blackberry device, containing geolocation, metadata,
 * geofence information, and other event-related details.
 * <p>
 * This class is used to deserialize incoming event data from Blackberry devices for processing
 * within the Trackmatic system. It supports key information such as device ID, asset info,
 * geofences, GPS locations, alarms, and event types.
 * </p>
 * <p>
 * Some fields related to door status, fence ID, handbrake, and impact are currently commented out
 * and may be added in future enhancements.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BlackberryEventRaw implements Serializable {

    private String formattedEvent;

    private String deviceId;
    private String identifier;

    private String assetid;

    private String asset_type_id;

//    private boolean door;

//    private String fenceid;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Geofence implements Serializable{
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        private String name;
    }

    private List<Geofence> geofences;

//    private boolean handbrake;

//    private boolean impact;

    public GeoLocation getLast_known_geo_location() {
        return last_known_geo_location;
    }

    public void setLast_known_geo_location(GeoLocation last_known_geo_location) {
        this.last_known_geo_location = last_known_geo_location;
    }

    private long ref_recorded_on;

    private GeoLocation geo_location;

    private GeoLocation last_known_geo_location;

    private String alarm;

    private String type;

    private int start_duration_delay;

    private int stop_duration;

    private TmMetadata tmMetadata;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getAssetid() {
        return assetid;
    }

    public void setAssetid(String assetid) {
        this.assetid = assetid;
    }

    public String getAsset_type_id() {
        return asset_type_id;
    }

    public void setAsset_type_id(String asset_type_id) {
        this.asset_type_id = asset_type_id;
    }

//    public boolean isDoor() {
//        return door;
//    }

//    public void setDoor(boolean door) {
//        this.door = door;
//    }

//    public String getFenceid() {
//        return fenceid;
//    }
//
//    public void setFenceid(String fenceid) {
//        this.fenceid = fenceid;
//    }

    public List<Geofence> getGeofences() {
        return geofences;
    }

    public void setGeofences(List<Geofence> geofences) {
        this.geofences = geofences;
    }

//    public boolean isHandbrake() {
//        return handbrake;
//    }
//
//    public void setHandbrake(boolean handbrake) {
//        this.handbrake = handbrake;
//    }
//
//    public boolean isImpact() {
//        return impact;
//    }
//
//    public void setImpact(boolean impact) {
//        this.impact = impact;
//    }


    public String getFormattedEvent() {
        return formattedEvent;
    }

    public void setFormattedEvent(String formattedEvent) {
        this.formattedEvent = formattedEvent;
    }

    public long getRef_recorded_on() {
        return ref_recorded_on;
    }

    public void setRef_recorded_on(long ref_recorded_on) {
        this.ref_recorded_on = ref_recorded_on;
    }

    public GeoLocation getGeo_location() {
        return geo_location;
    }

    public void setGeo_location(GeoLocation geo_location) {
        this.geo_location = geo_location;
    }

    public String getAlarm() {
        return alarm;
    }

    public void setAlarm(String alarm) {
        this.alarm = alarm;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getStart_duration_delay() {
        return start_duration_delay;
    }

    public void setStart_duration_delay(int start_duration_delay) {
        this.start_duration_delay = start_duration_delay;
    }

    public int getStop_duration() {
        return stop_duration;
    }

    public void setStop_duration(int stop_duration) {
        this.stop_duration = stop_duration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public TmMetadata getTmMetadata() {
        return tmMetadata;
    }

    public void setTmMetadata(TmMetadata tmMetadata) {
        this.tmMetadata = tmMetadata;
    }
}
