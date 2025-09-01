//package za.co.trackmatic.flink.thermoking;
//
//import za.co.trackmatic.flink.models.MultipleProviders.DoorStateEvent;
//import za.co.trackmatic.flink.events.thermoking.Event;
//import za.co.trackmatic.flink.models.MultipleProviders.LowFuelEvent;
//import za.co.trackmatic.flink.models.MultipleProviders.SetPointEvent;
//import za.co.trackmatic.flink.models.thermoking.ThermokingData;
//import za.co.trackmatic.flink.models.trackmatic.LatLong;
//
//import java.util.Date;
//import java.util.Objects;
//
//public class CacheManager {
//
//    private Event createSetpointEvent(String serial,
//                                      ThermokingData rawData,
//                                      double oldSetpoint,
//                                      double newSetpoint,
//                                      String subtype) {
//        //Logger.log("Creating setpoint event for " + serial + " of type " + subtype);
//        SetPointEvent spe = new SetPointEvent();
//        spe.setOldSetpoint(oldSetpoint);
//        spe.setNewSetpoint(newSetpoint);
//        spe.setTimestamp(new Date().getTime());
//        spe.setLocation(new LatLong(rawData.getLatitude(), rawData.getLongitude()));
//        spe.setSubtype(subtype);
//
//        Event event = new Event();
//        event.setSetPointEvent(spe);
//
//        return event;
//    }
//
//    private Event createLowFuelEvent(String serial, ThermokingData rawData){
//        LowFuelEvent lfe = new LowFuelEvent();
//
//        lfe.setFuelLevel(rawData.getFuelLevel());
//        lfe.setTimestamp(new Date().getTime());
//        lfe.setFuelTankSize(rawData.getFuelTankSize());
//
//        Event event = new Event();
//        event.setLowFuelEvent(lfe);
//
//        return event;
//    }
//
//    private StateAndEvents checkFuelLevel(String serial, ThermokingData rawData, StateAndEvents stateAndEvents){
//        if(rawData.getFuelLevel() < rawData.getFuelTankSize()*10/100){
//            stateAndEvents.getEvents().add(createLowFuelEvent(serial,rawData));
//        }
//        return stateAndEvents;
//    }
//
//    private StateAndEvents checkSetPointChanged(String serial, ThermokingData rawData, StateAndEvents stateAndEvents) {
//        ThermokingData old = stateAndEvents.getDeviceState().getThermokingData();
//
//        if (rawData.getSetPoint1() != null && old.getSetPoint1() != null && !Objects.equals(rawData.getSetPoint1(), old.getSetPoint1())) {
//             stateAndEvents.getEvents().add(createSetpointEvent(serial, rawData, old.getSetPoint1(), rawData.getSetPoint1(), SetPointEvent.SUBTYPE_SETPOINT1_CHANGED));
//        }
//
//        if (rawData.getSetPoint2() != null && old.getSetPoint2() != null && !Objects.equals(rawData.getSetPoint2(), old.getSetPoint2())) {
//            stateAndEvents.getEvents().add(createSetpointEvent(serial, rawData, old.getSetPoint2(), rawData.getSetPoint2(), SetPointEvent.SUBTYPE_SETPOINT2_CHANGED));
//        }
//
//        if (rawData.getSetPoint3() != null && old.getSetPoint3() != null && !Objects.equals(rawData.getSetPoint3(), old.getSetPoint3())) {
//            stateAndEvents.getEvents().add(createSetpointEvent(serial, rawData, old.getSetPoint3(), rawData.getSetPoint3(), SetPointEvent.SUBTYPE_SETPOINT3_CHANGED));
//        }
//
//        return stateAndEvents;
//    }
//
//    private Event createDoorEvent(String serial, ThermokingData rawData, String subtype) {
//        //System.out.println("Creating door event for " + serial + " of type " + subtype);
//
//        DoorStateEvent dse = new DoorStateEvent();
//        dse.setTimestamp(new Date().getTime());
//        dse.setLocation(new LatLong(rawData.getLatitude(), rawData.getLongitude()));
//        dse.setSubtype(subtype);
//
//        Event event = new Event();
//        event.setDoorStateEvent(dse);
//
//        return event;
//    }
//
//    private StateAndEvents checkDoorState(String serial, ThermokingData rawData, StateAndEvents stateAndEvents) {
//        ThermokingData old = stateAndEvents.getDeviceState().getThermokingData();
//
//        if (rawData.isZone1DoorOpen() && !old.isZone1DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE1_DOOR_OPEN));
//        }
//
//        if (!rawData.isZone1DoorOpen() && old.isZone1DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE1_DOOR_CLOSED));
//        }
//
//        if (rawData.isZone2DoorOpen() && !old.isZone2DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE2_DOOR_OPEN));
//        }
//
//        if (!rawData.isZone2DoorOpen() && old.isZone2DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE2_DOOR_CLOSED));
//        }
//        if (rawData.isZone3DoorOpen() && !old.isZone3DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE3_DOOR_OPEN));
//        }
//
//        if (!rawData.isZone3DoorOpen() && old.isZone3DoorOpen()) {
//            stateAndEvents.getEvents().add(createDoorEvent(serial, rawData, DoorStateEvent.SUBTYPE_ZONE3_DOOR_CLOSED));
//        }
//
//        return stateAndEvents;
//    }
//
//    public StateAndEvents process(String serial, ThermokingData rawData, StateAndEvents stateAndEvents) {
//        if (stateAndEvents.getDeviceState().getThermokingData() == null) {
//            /* initialize */
//            stateAndEvents.getDeviceState().setThermokingData(rawData);
//            return stateAndEvents;
//        }
//
//        stateAndEvents = checkSetPointChanged(serial, rawData, stateAndEvents);
//        stateAndEvents = checkDoorState(serial, rawData, stateAndEvents);
//        stateAndEvents = checkFuelLevel(serial, rawData, stateAndEvents);
//
//        stateAndEvents.getDeviceState().setThermokingData(rawData);
//
//        return stateAndEvents;
//    }
//}
