//package za.co.trackmatic.flink.thermoking;
//
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
//import za.co.trackmatic.flink.models.thermoking.ThermokingData;
//
//import java.io.Serializable;
//
//@JsonIgnoreProperties(ignoreUnknown = true)
//public class DeviceState implements Serializable {
//
//    private GeofenceState geofenceState = new GeofenceState();
//
//    private ThermokingData thermokingData = null;
//
//    public DeviceState() {
//
//    }
//
//    public GeofenceState getGeofenceState() {
//        return geofenceState;
//    }
//
//    public void setGeofenceState(GeofenceState geofenceState) {
//        this.geofenceState = geofenceState;
//    }
//
//    public ThermokingData getThermokingData() {
//        return thermokingData;
//    }
//
//    public void setThermokingData(ThermokingData thermokingData) {
//        this.thermokingData = thermokingData;
//    }
//}
