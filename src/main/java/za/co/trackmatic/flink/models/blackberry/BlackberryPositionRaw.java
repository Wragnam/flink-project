package za.co.trackmatic.flink.models.blackberry;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the raw position data received from a Blackberry tracking device,
 * including GPS location, sensor data, device state, alerts, and associated metadata.
 * This class serves as a data model for deserializing position updates, supporting detailed
 * attributes like horizontal accuracy (hacc), dilution of precision (hdop, pdop),
 * altitude, battery and door states, geofences, rules, and orientation.
 * Contains several nested static classes to represent complex structures such as:
 * <ul>
 *     <li>{@link Persist} — Tracks what attributes have changed since the last update.</li>
 *     <li>{@link Orientation} — Holds orientation data with old and new values and associated location.</li>
 *     <li>{@link Asset} — Represents asset details linked to this position data.</li>
 *     <li>{@link Rules} — Defines alert settings and frequency configurations.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BlackberryPositionRaw implements Serializable {

    public BlackberryPositionRaw() {
    }

    private String deviceId;

    private Long created;

    private String alarm;

    private String travel_mode;

    private String logged_gps_fix;

    private String logged_gps;

    private GeoLocation geo_location;

    private double hacc;

    private double snv;

    private double hdop;

    private double pdop;

    private double altitude;

    private Integer state;

    private Boolean battery_survive_mode;

    private List<GeoLocation> geofences;

    private Rules rules;

    private Boolean battery_state;

    private Boolean door;

    private Asset asset;

    private Orientation orientation;

    private Persist persist;

    private TmMetadata metadata;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }

    public String getAlarm() {
        return alarm;
    }

    public void setAlarm(String alarm) {
        this.alarm = alarm;
    }

    public String getTravel_mode() {
        return travel_mode;
    }

    public void setTravel_mode(String travel_mode) {
        this.travel_mode = travel_mode;
    }

    public String getLogged_gps_fix() {
        return logged_gps_fix;
    }

    public void setLogged_gps_fix(String logged_gps_fix) {
        this.logged_gps_fix = logged_gps_fix;
    }

    public String getLogged_gps() {
        return logged_gps;
    }

    public void setLogged_gps(String logged_gps) {
        this.logged_gps = logged_gps;
    }

    public GeoLocation getGeo_location() {
        return geo_location;
    }

    public void setGeo_location(GeoLocation geo_location) {
        this.geo_location = geo_location;
    }

    public double getHacc() {
        return hacc;
    }

    public void setHacc(double hacc) {
        this.hacc = hacc;
    }

    public double getSnv() {
        return snv;
    }

    public void setSnv(double snv) {
        this.snv = snv;
    }

    public double getHdop() {
        return hdop;
    }

    public void setHdop(double hdop) {
        this.hdop = hdop;
    }

    public double getPdop() {
        return pdop;
    }

    public void setPdop(double pdop) {
        this.pdop = pdop;
    }

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public Boolean getBattery_survive_mode() {
        return battery_survive_mode;
    }

    public void setBattery_survive_mode(Boolean battery_survive_mode) {
        this.battery_survive_mode = battery_survive_mode;
    }

    public List<GeoLocation> getGeofences() {
        return geofences;
    }

    public void setGeofences(List<GeoLocation> geofences) {
        this.geofences = geofences;
    }

    public Rules getRules() {
        return rules;
    }

    public void setRules(Rules rules) {
        this.rules = rules;
    }

    public Boolean getBattery_state() {
        return battery_state;
    }

    public void setBattery_state(Boolean battery_state) {
        this.battery_state = battery_state;
    }

    public Boolean getDoor() {
        return door;
    }

    public void setDoor(Boolean door) {
        this.door = door;
    }

    public Asset getAsset() {
        return asset;
    }

    public void setAsset(Asset asset) {
        this.asset = asset;
    }

    public Orientation getOrientation() {
        return orientation;
    }

    public void setOrientation(Orientation orientation) {
        this.orientation = orientation;
    }

    public Persist getPersist() {
        return persist;
    }

    public void setPersist(Persist persist) {
        this.persist = persist;
    }

    public static class Persist implements Serializable {
        private Integer state_changed;
        private Integer battery_state_changed;
        private Integer battery_survive_mode_changed;
        private Integer contents_changed;
        private Integer contents_percentage_changed;
        private Integer door_changed;
        private Integer orientation_changed;
        private Integer alert_cleared;

        public Integer getState_changed() {
            return state_changed;
        }

        public void setState_changed(Integer state_changed) {
            this.state_changed = state_changed;
        }

        public Integer getBattery_state_changed() {
            return battery_state_changed;
        }

        public void setBattery_state_changed(Integer battery_state_changed) {
            this.battery_state_changed = battery_state_changed;
        }

        public Integer getBattery_survive_mode_changed() {
            return battery_survive_mode_changed;
        }

        public void setBattery_survive_mode_changed(Integer battery_survive_mode_changed) {
            this.battery_survive_mode_changed = battery_survive_mode_changed;
        }

        public Integer getContents_changed() {
            return contents_changed;
        }

        public void setContents_changed(Integer contents_changed) {
            this.contents_changed = contents_changed;
        }

        public Integer getContents_percentage_changed() {
            return contents_percentage_changed;
        }

        public void setContents_percentage_changed(Integer contents_percentage_changed) {
            this.contents_percentage_changed = contents_percentage_changed;
        }

        public Integer getDoor_changed() {
            return door_changed;
        }

        public void setDoor_changed(Integer door_changed) {
            this.door_changed = door_changed;
        }

        public Integer getOrientation_changed() {
            return orientation_changed;
        }

        public void setOrientation_changed(Integer orientation_changed) {
            this.orientation_changed = orientation_changed;
        }

        public Integer getAlert_cleared() {
            return alert_cleared;
        }

        public void setAlert_cleared(Integer alert_cleared) {
            this.alert_cleared = alert_cleared;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Orientation implements Serializable {
        private GeoLocation geo_location;

        private Integer new_orientation;

        private Integer old_orientation;

        public GeoLocation getGeo_location() {
            return geo_location;
        }

        public void setGeo_location(GeoLocation geo_location) {
            this.geo_location = geo_location;
        }

        public double getNew_orientation() {
            return new_orientation;
        }

        public void setNew_orientation(Integer new_orientation) {
            this.new_orientation = new_orientation;
        }

        public double getOld_orientation() {
            return old_orientation;
        }

        public void setOld_orientation(Integer old_orientation) {
            this.old_orientation = old_orientation;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Asset implements Serializable {

        private Param param;

        public Param getParam() {
            return param;
        }

        public void setParam(Param param) {
            this.param = param;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Param implements Serializable {
            private String identifier;

            public String getIdentifier() {
                return identifier;
            }

            public void setIdentifier(String identifier) {
                this.identifier = identifier;
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Rules implements Serializable {
        private Boolean alert_battery;

        private Boolean alert_door;

        private Boolean alert_humid;

        private Boolean alert_cargo;

        private Boolean alert_container;

        private Boolean alert_stop;

        private Boolean alert_tamper;

        private Boolean alert_lost_module;

        private Frequency frequency;

        public Boolean getAlert_battery() {
            return alert_battery;
        }

        public void setAlert_battery(Boolean alert_battery) {
            this.alert_battery = alert_battery;
        }

        public Boolean getAlert_door() {
            return alert_door;
        }

        public void setAlert_door(Boolean alert_door) {
            this.alert_door = alert_door;
        }

        public Boolean getAlert_humid() {
            return alert_humid;
        }

        public void setAlert_humid(Boolean alert_humid) {
            this.alert_humid = alert_humid;
        }

        public Boolean getAlert_cargo() {
            return alert_cargo;
        }

        public void setAlert_cargo(Boolean alert_cargo) {
            this.alert_cargo = alert_cargo;
        }

        public Boolean getAlert_container() {
            return alert_container;
        }

        public void setAlert_container(Boolean alert_container) {
            this.alert_container = alert_container;
        }

        public Boolean getAlert_stop() {
            return alert_stop;
        }

        public void setAlert_stop(Boolean alert_stop) {
            this.alert_stop = alert_stop;
        }

        public Boolean getAlert_tamper() {
            return alert_tamper;
        }

        public void setAlert_tamper(Boolean alert_tamper) {
            this.alert_tamper = alert_tamper;
        }

        public Boolean getAlert_lost_module() {
            return alert_lost_module;
        }

        public void setAlert_lost_module(Boolean alert_lost_module) {
            this.alert_lost_module = alert_lost_module;
        }

        public Frequency getFrequency() {
            return frequency;
        }

        public void setFrequency(Frequency frequency) {
            this.frequency = frequency;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Frequency implements Serializable {
            private String label;

            private String il8nKey;

            private Params moving;

            private Params stopped;

            private List<String> allowed_types;

            private Object options;

            public String getLabel() {
                return label;
            }

            public void setLabel(String label) {
                this.label = label;
            }

            public String getIl8nKey() {
                return il8nKey;
            }

            public void setIl8nKey(String il8nKey) {
                this.il8nKey = il8nKey;
            }

            public Params getMoving() {
                return moving;
            }

            public void setMoving(Params moving) {
                this.moving = moving;
            }

            public Params getStopped() {
                return stopped;
            }

            public void setStopped(Params stopped) {
                this.stopped = stopped;
            }

            public List<String> getAllowed_types() {
                return allowed_types;
            }

            public void setAllowed_types(List<String> allowed_types) {
                this.allowed_types = allowed_types;
            }

            public Object getOptions() {
                return options;
            }

            public void setOptions(Object options) {
                this.options = options;
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            public static class Params implements Serializable {
                private Integer param1;
                private Integer param2;
                private Integer param3;
                private Integer param4;

                public Integer getParam1() {
                    return param1;
                }

                public void setParam1(Integer param1) {
                    this.param1 = param1;
                }

                public Integer getParam2() {
                    return param2;
                }

                public void setParam2(Integer param2) {
                    this.param2 = param2;
                }

                public Integer getParam3() {
                    return param3;
                }

                public void setParam3(Integer param3) {
                    this.param3 = param3;
                }

                public Integer getParam4() {
                    return param4;
                }

                public void setParam4(Integer param4) {
                    this.param4 = param4;
                }
            }
        }
    }


}
