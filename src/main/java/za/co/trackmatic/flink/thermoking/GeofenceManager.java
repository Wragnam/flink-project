//package za.co.trackmatic.flink.thermoking;
//
//import org.apache.commons.math3.util.Pair;
//import za.co.trackmatic.flink.CacheUtils;
//import za.co.trackmatic.flink.Utils.Utils;
//import za.co.trackmatic.flink.events.thermoking.Event;
//import za.co.trackmatic.flink.models.trackmatic.*;
//import za.co.trackmatic.flink.models.thermoking.ThermokingData;
//
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//
//public class GeofenceManager {
//
//    public GeofenceManager() {
//    }
//
//
////    private Event createGeofenceEvent(ThermokingData rawData, GeofenceToBeCreated geofenceToBeCreated) {
////        GeofenceEvent geofenceEvent = new GeofenceEvent();
////        geofenceEvent.setType(geofenceToBeCreated.getType());
////        geofenceEvent.setLocation(new LatLong(rawData.getLatitude(), rawData.getLongitude()));
////        geofenceEvent.setGeofenceId(geofenceToBeCreated.getId());
////        geofenceEvent.setGeofenceName(geofenceToBeCreated.getName());
////        geofenceEvent.setTimestamp(new Date().getTime());
////
////        Event event = new Event();
////        event.setGeofenceEvent(geofenceEvent);
////
////        return event;
////    }
//
////    public StateAndEvents processGeofences(String server, String orgId, ThermokingData data, StateAndEvents stateAndEvents, Map<String,String> mapping) {
////        LatLong point = new LatLong(data.getLatitude(), data.getLongitude());
////
////        List<GeofenceItem> fences = CacheUtils.isPointInGeofences(server,orgId,point,data.getTmMetadata().getSiteIds());
////        Pair<List<GeofenceToBeCreated>, Map<String,String>> geofenceData = Utils.geofenceEvent(mapping,fences, data.getSpeed(), Objects.equals(data.getIgnitionStatus(), "on"));
////        for(GeofenceToBeCreated geofenceItem: geofenceData.getFirst()){
////            Event ev = createGeofenceEvent(data,geofenceItem);
////            stateAndEvents.getEvents().add(ev);
////        }
////
////        return stateAndEvents;
////    }
//}
