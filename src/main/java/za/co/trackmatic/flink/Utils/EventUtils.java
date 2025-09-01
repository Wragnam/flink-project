package za.co.trackmatic.flink.Utils;

import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.universal.BatteryEvent;
import za.co.trackmatic.flink.models.MultipleProviders.DoorStateEvent;
import za.co.trackmatic.flink.models.MultipleProviders.EventState;
import za.co.trackmatic.flink.models.MultipleProviders.LowFuelEvent;
import za.co.trackmatic.flink.models.MultipleProviders.SetPointEvent;
import za.co.trackmatic.flink.models.trackmatic.*;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Helper class, containing all functions that are used to create events
 */
public class EventUtils implements Serializable {

    public EventUtils() {
    }

    /**
     * Creates and handles an unknown stop event for a `GeneralEvent`, adjusting the event type
     * and state to indicate an unknown stop occurrence, based on ignition status and geofence presence.
     *
     * @param mainGeneralEvent The general event that may represent an unknown stop.
     * @param ignition         The ignition status for stop detection.
     * @param collector        The collector used to emit the event if an unknown stop is detected.
     * @param state            The current state of the event, tracking unknown stop occurrences.
     * @param metadata         The metadata for setting control rooms.
     * @param fences           A list of geofence items, where a non-empty list implies known geofences.
     * @param dataSource       The source of the data (i.e. Thermoking)
     */
    public static void createUnknownStopEvent(GeneralEvent mainGeneralEvent, Integer ignition, Collector<GeneralEvent> collector, EventState state, TmMetadata metadata, List<GeofenceItem> fences, String dataSource) {
        // Check if there are no geofences, indicating an unknown location
        if (fences == null || fences.isEmpty()) {
            // Trigger an unknown stop event if the ignition (`Ig`) is off (0) and no stop is currently active
            if (ignition == 0 && state.getUnknownStopStatus() == 0) {
                // Update state to reflect that an unknown stop is starting
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                state.setUnknownStopStatus(1);
                state.getUnknownStopEvent().setStartTime(generalEvent.getCreated());

                // Set the event type to "UNKNOWN_STOP" and associate control rooms
                generalEvent.setType("UNKNOWN_STOP");

                generalEvent.setUnknownStopEvent(state.getUnknownStopEvent());
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                // Collect the event to emit it downstream
                collector.collect(generalEvent);
            }
        }


        // Check if the ignition has turned on, marking the end of the unknown stop
        if (ignition == 1 && state.getUnknownStopStatus() == 1) {

            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            // Reset unknown stop status to 0 (no active stop)
            state.setUnknownStopStatus(0);

            // Set the end time of the unknown stop event and calculate its duration
            state.getUnknownStopEvent().setEndTime(generalEvent.getCreated());
            state.getUnknownStopEvent().setDuration(state.getUnknownStopEvent().getEndTime() - state.getUnknownStopEvent().getStartTime());

            // Update the event type and set the unknown stop event details
            generalEvent.setType("UNKNOWN_STOP");

            generalEvent.setUnknownStopEvent(state.getUnknownStopEvent());

            // Reset the unknown stop event in the state for future use
            state.setUnknownStopEvent(new StopEvent());

            // Collect the event to emit it downstream
            collector.collect(generalEvent);
        }
    }


    public static void createGeofenceEvent(GeneralEvent mainGeneralEvent, GeofenceToBeCreated geofenceToBeCreated,
                                           boolean checkUnplanned, Date date, EventState state, TmMetadata metadata, String source, Collector<GeneralEvent> collector) {
        // Create a new GeofenceEvent and populate it with location, ID, timestamp, type, and name from the geofence
        GeofenceEvent geoEvent = new GeofenceEvent();
        geoEvent.setLocation(new LatLong(mainGeneralEvent.getLatitude(), mainGeneralEvent.getLongitude()));
        geoEvent.setGeofenceId(geofenceToBeCreated.getId());
        geoEvent.setTimestamp(date.getTime());
        geoEvent.setType(geofenceToBeCreated.getType());
        geoEvent.setGeofenceName(geofenceToBeCreated.getName());

        GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
        generalEvent.setGeofenceEvent(geoEvent); // Associate with the general event
        generalEvent.setLocationName(geofenceToBeCreated.getName());
        generalEvent.setLocationId(geofenceToBeCreated.getId());

        // Check if the event should be marked as "UNPLANNED_STOP" if `checkUnplanned` is true
        if (checkUnplanned || state.isUnplannedStop()) {
            if (Objects.equals(geoEvent.getType(), GeofenceEvent.SUBTYPE_ARRIVAL) && !state.isUnplannedStop()) {
                generalEvent.setType("UNPLANNED_STOP");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                state.getUnplannedStopEvent().setStartTime(generalEvent.getCreated());
                state.setUnplannedStop(true);
                state.setUnplannedStopId(geofenceToBeCreated.getId());
                generalEvent.setUnknownStopEvent(state.getUnplannedStopEvent());
                collector.collect(generalEvent);
                return;
            } else if (state.getUnplannedStopId() != null && Objects.equals(state.getUnplannedStopId(), geofenceToBeCreated.getId()) && state.isUnplannedStop()) {
                generalEvent.setType("UNPLANNED_STOP");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                state.getUnplannedStopEvent().setEndTime(generalEvent.getCreated());
                state.getUnplannedStopEvent().setDuration(state.getUnplannedStopEvent().getEndTime() - state.getUnplannedStopEvent().getStartTime());
                generalEvent.setUnknownStopEvent(state.getUnplannedStopEvent());
                state.setUnplannedStop(false);
                state.setUnplannedStopId("");
                state.setUnplannedStopEvent(new StopEvent());
                collector.collect(generalEvent);
                return;
            }
        }

        // If there are no stop IDs in metadata, mark it as a "GEOFENCE" event and collect
        if (metadata.getStopIds() == null) {
            generalEvent.setType("GEOFENCE");
            setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
            collector.collect(generalEvent);
        } else {
            // Otherwise, check if the geofence ID is listed in metadata stop IDs
            if (metadata.getStopIds().contains(geofenceToBeCreated.getId())) {
                generalEvent.setType("GEOFENCE");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                collector.collect(generalEvent);
            }
        }
    }

    public static void createGeofenceEvent(GeneralEvent mainGeneralEvent, GeofenceToBeCreated geofenceToBeCreated,
                                           boolean checkUnplanned, long utcTime, EventState state, TmMetadata metadata, String source, Collector<GeneralEvent> collector) {
        // Create a new GeofenceEvent and populate it with location, ID, timestamp, type, and name from the geofence
        GeofenceEvent geoEvent = new GeofenceEvent();
        geoEvent.setLocation(new LatLong(mainGeneralEvent.getLatitude(), mainGeneralEvent.getLongitude()));
        geoEvent.setGeofenceId(geofenceToBeCreated.getId());
        geoEvent.setTimestamp(utcTime);
        geoEvent.setType(geofenceToBeCreated.getType());
        geoEvent.setGeofenceName(geofenceToBeCreated.getName());

        GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
        generalEvent.setGeofenceEvent(geoEvent); // Associate with the general event
        generalEvent.setLocationName(geofenceToBeCreated.getName());
        generalEvent.setLocationId(geofenceToBeCreated.getId());

        // Check if the event should be marked as "UNPLANNED_STOP" if `checkUnplanned` is true
        if (checkUnplanned || state.isUnplannedStop()) {
            if (Objects.equals(geoEvent.getType(), GeofenceEvent.SUBTYPE_ARRIVAL) && !state.isUnplannedStop()) {
                generalEvent.setType("UNPLANNED_STOP");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                state.getUnplannedStopEvent().setStartTime(generalEvent.getCreated());
                state.setUnplannedStop(true);
                state.setUnplannedStopId(geofenceToBeCreated.getId());
                generalEvent.setUnknownStopEvent(state.getUnplannedStopEvent());
                collector.collect(generalEvent);
                return;
            } else if (state.getUnplannedStopId() != null && Objects.equals(state.getUnplannedStopId(), geofenceToBeCreated.getId()) && state.isUnplannedStop()) {
                generalEvent.setType("UNPLANNED_STOP");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                state.getUnplannedStopEvent().setEndTime(generalEvent.getCreated());
                state.getUnplannedStopEvent().setDuration(state.getUnplannedStopEvent().getEndTime() - state.getUnplannedStopEvent().getStartTime());
                generalEvent.setUnknownStopEvent(state.getUnplannedStopEvent());
                state.setUnplannedStop(false);
                state.setUnplannedStopId("");
                state.setUnplannedStopEvent(new StopEvent());
                collector.collect(generalEvent);
                return;
            }
        }

        // If there are no stop IDs in metadata, mark it as a "GEOFENCE" event and collect
        if (metadata.getStopIds() == null) {
            generalEvent.setType("GEOFENCE");
            setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
            collector.collect(generalEvent);
        } else {
            // Otherwise, check if the geofence ID is listed in metadata stop IDs
            if (metadata.getStopIds().contains(geofenceToBeCreated.getId())) {
                generalEvent.setType("GEOFENCE");
                setControlRooms(generalEvent, metadata, source); // Set control rooms for the event
                collector.collect(generalEvent);
            }
        }
    }

    /**
     * Creates and handles a panic event, updating the event's state and
     * notifying control rooms if a panic condition is met.
     *
     * @param mainGeneralEvent The event that potentially represents a panic situation.
     * @param state            The current state of the event, tracking the last panic state and time.
     * @param panic            The state of panic as a Boolean value (nullable).
     * @param collector        The collector used to emit the event if a panic is detected.
     * @param metadata         The metadata for setting control rooms.
     * @param dataSource       The source of the data (i.e. Thermoking)
     */
    public static void createPanicEvent(GeneralEvent mainGeneralEvent, EventState state, Boolean panic, Collector<GeneralEvent> collector, TmMetadata metadata, String dataSource) {
        // Check if the panic condition in the data item is set
        if (panic != null && panic) {
            // Only trigger panic if the previous panic state was inactive (0)
            // and more than 30 minutes have passed since the last panic event
            if (state.getPanic() == 0 && mainGeneralEvent.getCreated() - state.getLastPanicTime() > 1800) {

                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);

                generalEvent.setType("PANIC");

                // Update the state to reflect that a panic is now active
                state.setPanic(1);

                // Update the last panic time to the current processed time
                state.setLastPanicTime(generalEvent.getCreated());
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                // Collect the updated event to notify downstream processes
                collector.collect(generalEvent);
            }
        } else {
            // Reset panic state to inactive (0) if no panic condition is set
            state.setPanic(0);
        }
    }

    /**
     * Creates and emits a main battery event based on the battery level and previous state.
     *
     * <p>Logic:
     * <ul>
     *   <li>If battery level is exactly 0.0, it is considered a disconnection event ("BATTERY_DISCONNECT").</li>
     *   <li>If battery level is below 11.5V, it is considered a low voltage event ("BATTERY_LOW").</li>
     *   <li>State counters ensure events are not emitted repeatedly unless conditions change.</li>
     *   <li>If voltage returns to normal, the state counter is reset.</li>
     * </ul>
     *
     * @param mainGeneralEvent The base event to clone and enrich.
     * @param mainBatteryLevel The current main battery voltage level.
     * @param collector        The Flink collector to emit resulting events.
     * @param state            The current event state for suppressing duplicate events.
     * @param metadata         TM metadata used for assigning control rooms.
     * @param dataSource       Source identifier used in event tagging.
     */
    public static void createMainBatteryEvent(GeneralEvent mainGeneralEvent, Double mainBatteryLevel, Collector<GeneralEvent> collector, EventState state, TmMetadata metadata, String dataSource) {
        // Check for main battery disconnection (voltage 0.0) and create event if not already reported
        if (mainBatteryLevel == 0.0) {
            if (state.getMainBatteryEventCounter() != 2) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_DISCONNECT");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("MAIN_BATTERY");
                batteryEvent.setVoltage(0.0);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setMainBatteryEventCounter(2);
                collector.collect(generalEvent);
            }
        }
        // Check if main battery voltage is low (< 11.5) and create event if not reported recently
        else if (mainBatteryLevel < 11.5) {
            if (state.getMainBatteryEventCounter() == 0) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_LOW");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("MAIN_BATTERY");
                batteryEvent.setVoltage(mainBatteryLevel);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setMainBatteryEventCounter(1);
                collector.collect(generalEvent);
            }
        }
        // Reset counter if voltage is back to normal
        else {
            state.setMainBatteryEventCounter(0);
        }
    }

    /**
     * Creates and emits a backup battery event based on the battery level and previous state.
     *
     * <p>Logic:
     * <ul>
     *   <li>If battery level is exactly 0.0, it is considered a disconnection event ("BATTERY_DISCONNECT").</li>
     *   <li>If battery level is below 11.5V, it is considered a low voltage event ("BATTERY_LOW").</li>
     *   <li>State counters ensure events are not emitted repeatedly unless conditions change.</li>
     *   <li>If voltage returns to normal, the state counter is reset.</li>
     * </ul>
     *
     * @param mainGeneralEvent The base event to clone and enrich.
     * @param backupBatteryLevel The current backup battery voltage level.
     * @param collector          The Flink collector to emit resulting events.
     * @param state              The current event state for suppressing duplicate events.
     * @param metadata           TM metadata used for assigning control rooms.
     * @param dataSource         Source identifier used in event tagging.
     */
    public static void createBackupBatteryEvent(GeneralEvent mainGeneralEvent, Double backupBatteryLevel, Collector<GeneralEvent> collector, EventState state, TmMetadata metadata, String dataSource) {
        // Check for main battery disconnection (voltage 0.0) and create event if not already reported
        if (backupBatteryLevel == 0.0) {
            if (state.getBackupBatteryEventCounter() != 2) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_DISCONNECT");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("BACKUP_BATTERY");
                batteryEvent.setVoltage(0.0);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setBackupBatteryEventCounter(2);
                collector.collect(generalEvent);
            }
        }
        // Check if main battery voltage is low (< 11.5) and create event if not reported recently
        else if (backupBatteryLevel < 11.5) {
            if (state.getBackupBatteryEventCounter() == 0) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_LOW");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("BACKUP_BATTERY");
                batteryEvent.setVoltage(backupBatteryLevel);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setBackupBatteryEventCounter(1);
                collector.collect(generalEvent);
            }
        }
        // Reset counter if voltage is back to normal
        else {
            state.setBackupBatteryEventCounter(0);
        }
    }

    /**
     * Creates a SetPointEvent with the provided setpoint details, location, and subtype.
     * The event includes information about the old and new setpoints, location coordinates,
     * timestamp, and subtype.
     *
     * @param oldSetpoint the old setpoint value.
     * @param newSetpoint the new setpoint value.
     * @param lat         the latitude of the location.
     * @param lon         the longitude of the location.
     * @param subtype     the subtype of the setpoint event.
     * @return a SetPointEvent object containing the provided details.
     */
    private static SetPointEvent createSetpointEvent(double oldSetpoint, double newSetpoint, double lat, double lon, String subtype) {
        // Create a new SetPointEvent object
        SetPointEvent spe = new SetPointEvent();

        // Set the old setpoint value
        spe.setOldSetpoint(oldSetpoint);

        // Set the new setpoint value
        spe.setNewSetpoint(newSetpoint);
        spe.setTimestamp(new Date().getTime());
        spe.setLocation(new LatLong(lat, lon));
        spe.setSubtype(subtype);

        // Return the created SetPointEvent object
        return spe;
    }

    /**
     * Collects and emits setpoint change events if any of the current setpoints (1–3) differ from their previous values.
     * <p>
     * Each change results in a {@link SetPointEvent} that is emitted as a separate {@link GeneralEvent}.
     *
     * @param old              Previous state containing last known setpoint values.
     * @param setPoint1        Current setpoint 1 value (nullable).
     * @param setPoint2        Current setpoint 2 value (nullable).
     * @param setPoint3        Current setpoint 3 value (nullable).
     * @param mainGeneralEvent The base event to clone and enrich.
     * @param collector        The Flink collector used to emit events.
     * @param metadata         Metadata for assigning control rooms.
     * @param source           Source tag to annotate the event.
     */
    public static void collectSetpointEvents(EventState old, Double setPoint1, Double setPoint2,
                                             Double setPoint3, GeneralEvent mainGeneralEvent,
                                             Collector<GeneralEvent> collector, TmMetadata metadata,
                                             String source) {
        Double prevSetPoint1 = old.getSetPoint1();
        Double prevSetPoint2 = old.getSetPoint2();
        Double prevSetPoint3 = old.getSetPoint3();
        double lat = mainGeneralEvent.getLatitude();
        double lon = mainGeneralEvent.getLongitude();

        // Check if Setpoint 1 has changed
        if (setPoint1 != null && prevSetPoint1 != null && !Objects.equals(setPoint1, prevSetPoint1)) {
            // Create and set setpoint 1 changed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(SetPointEvent.TYPE);
            generalEvent.setSetPointEvent(createSetpointEvent(prevSetPoint1, setPoint1, lat, lon, SetPointEvent.SUBTYPE_SETPOINT1_CHANGED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if Setpoint 2 has changed
        if (setPoint2 != null && prevSetPoint2 != null && !Objects.equals(setPoint2, prevSetPoint2)) {
            // Create and set setpoint 2 changed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(SetPointEvent.TYPE);
            generalEvent.setSetPointEvent(createSetpointEvent(prevSetPoint2, setPoint2, lat, lon, SetPointEvent.SUBTYPE_SETPOINT2_CHANGED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if Setpoint 3 has changed
        if (setPoint3 != null && prevSetPoint3 != null && !Objects.equals(setPoint3, prevSetPoint3)) {
            // Create and set setpoint 3 changed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(SetPointEvent.TYPE);
            generalEvent.setSetPointEvent(createSetpointEvent(prevSetPoint3, setPoint3, lat, lon, SetPointEvent.SUBTYPE_SETPOINT3_CHANGED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }
    }

    /**
     * Constructs a new {@link DoorStateEvent} with the current timestamp, location, and subtype.
     *
     * @param lat     The latitude of the event.
     * @param lon     The longitude of the event.
     * @param subtype The subtype of the door event (e.g., "REAR_DOOR_OPEN").
     * @return A populated {@link DoorStateEvent} instance.
     */
    private static DoorStateEvent createDoorEvent(double lat, double lon, String subtype) {
        // Create a new instance of DoorStateEvent
        DoorStateEvent dse = new DoorStateEvent();

        // Set the new event's information
        dse.setTimestamp(new Date().getTime());
        dse.setLocation(new LatLong(lat, lon));
        dse.setSubtype(subtype);

        // Return the created DoorStateEvent object
        return dse;
    }

    /**
     * Collects and emits door state change events for each relevant door (rear, side, zone1–zone3).
     * <p>
     * This method detects transitions from open → closed and closed → open and emits events accordingly.
     *
     * @param rearDoorOpen     Current rear door state (open/closed/null).
     * @param sideDoorOpen     Current side door state.
     * @param zone1DoorOpen    Current zone 1 door state.
     * @param zone2DoorOpen    Current zone 2 door state.
     * @param zone3DoorOpen    Current zone 3 door state.
     * @param mainGeneralEvent The base event used to construct emitted events.
     * @param old              The previously known door states (for comparison).
     * @param collector        The Flink collector to emit events.
     * @param metadata         Metadata used to tag events with control room info.
     * @param source           Source identifier used for tracking/reporting.
     */
    public static void collectDoorStateEvents(Boolean rearDoorOpen, Boolean sideDoorOpen,
                                              Boolean zone1DoorOpen, Boolean zone2DoorOpen,
                                              Boolean zone3DoorOpen,
                                              GeneralEvent mainGeneralEvent, EventState old,
                                              Collector<GeneralEvent> collector, TmMetadata metadata,
                                              String source) {
        Boolean prevRearDoorOpen = old.getRearDoorOpen();
        Boolean prevSideDoorOpen = old.getSideDoorOpen();
        Boolean prevZone1DoorOpen = old.getZone1DoorOpen();
        Boolean prevZone2DoorOpen = old.getZone2DoorOpen();
        Boolean prevZone3DoorOpen = old.getZone3DoorOpen();
        double lat = mainGeneralEvent.getLatitude();
        double lon = mainGeneralEvent.getLongitude();

        // Check if the rear door has changed from closed to open
        if (rearDoorOpen != null && prevRearDoorOpen != null && rearDoorOpen && !prevRearDoorOpen) {
            // Create and set rear door open event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_REAR_DOOR_OPEN));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the rear door has changed from open to closed
        if (rearDoorOpen != null && prevRearDoorOpen != null && !rearDoorOpen && prevRearDoorOpen) {
            // Create and set rear door closed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_REAR_DOOR_CLOSED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the side door has changed from closed to open
        if (sideDoorOpen != null && prevSideDoorOpen != null && sideDoorOpen && !prevSideDoorOpen) {
            // Create and set side door open event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_SIDE_DOOR_OPEN));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the side door has changed from open to closed
        if (sideDoorOpen != null && prevSideDoorOpen != null && !sideDoorOpen && prevSideDoorOpen) {
            // Create and set side door closed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_SIDE_DOOR_CLOSED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 1 door has changed from closed to open
        if (zone1DoorOpen != null && prevZone1DoorOpen != null && zone1DoorOpen && !prevZone1DoorOpen) {
            // Create and set zone 1 door open event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE1_DOOR_OPEN));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 1 door has changed from open to closed
        if (zone1DoorOpen != null && prevZone1DoorOpen != null && !zone1DoorOpen && prevZone1DoorOpen) {
            // Create and set zone 1 door closed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE1_DOOR_CLOSED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 2 door has changed from closed to open
        if (zone2DoorOpen != null && prevZone2DoorOpen != null && zone2DoorOpen && !prevZone2DoorOpen) {
            // Create and set zone 2 door open event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE2_DOOR_OPEN));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 2 door has changed from open to closed
        if (zone2DoorOpen != null && prevZone2DoorOpen != null && !zone2DoorOpen && prevZone2DoorOpen) {
            // Create and set zone 2 door closed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE2_DOOR_CLOSED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 3 door has changed from closed to open
        if (zone3DoorOpen != null && prevZone3DoorOpen != null && zone3DoorOpen && !prevZone3DoorOpen) {
            // Create and set zone 3 door open event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE3_DOOR_OPEN));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }

        // Check if the zone 3 door has changed from open to closed
        if (zone3DoorOpen != null && prevZone3DoorOpen != null && !zone3DoorOpen && prevZone3DoorOpen) {
            // Create and set zone 3 door closed event
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType(DoorStateEvent.TYPE);
            generalEvent.setDoorStateEvent(createDoorEvent(lat, lon, DoorStateEvent.SUBTYPE_ZONE3_DOOR_CLOSED));
            EventUtils.setControlRooms(generalEvent, metadata, source);
            collector.collect(generalEvent);
        }
    }

    /**
     * Creates and emits a low fuel event when the fuel level is detected to be low.
     * <p>
     * The event includes rounded fuel level, tank size, timestamp, and is tagged with control room info.
     * After emission, updates the event state to indicate the fuel level is low.
     *
     * @param fuelLevel        The current fuel level.
     * @param fuelTankSize     The size of the fuel tank.
     * @param mainGeneralEvent The base event to clone and enrich.
     * @param collector        The Flink collector used to emit events.
     * @param state            The current event state to update fuel level status.
     * @param metadata         Metadata used to assign control rooms.
     * @param source           Source identifier for tagging the event.
     */
    public static void createLowFuelEvent(Double fuelLevel, Integer fuelTankSize,
                                          GeneralEvent mainGeneralEvent,
                                          Collector<GeneralEvent> collector, EventState state,
                                          TmMetadata metadata, String source) {
        GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);

        // Set the event type to "FUEL_LEVEL_LOW"
        generalEvent.setType("FUEL_LEVEL_LOW");

        // Set control room IDs in the general event based on raw data
        EventUtils.setControlRooms(generalEvent, metadata, source);

        // Create a LowFuelEvent and set its properties
        LowFuelEvent fe = new LowFuelEvent();
        fe.setFuelLevel((int) Math.round(fuelLevel));// Round fuel level to the nearest integer
        fe.setTimestamp(new Date().getTime());
        fe.setFuelTankSize(fuelTankSize);// Set default fuel tank size (could be dynamic based on raw data)

        // Attach the LowFuelEvent to the general event
        generalEvent.setLowFuelEvent(fe);

        // Collect the event
        collector.collect(generalEvent);

        // Clear the low fuel event after collection
        generalEvent.setLowFuelEvent(null);

        // Update the state to reflect that the fuel level is low
        state.setFuelLevelLow(true);

    }

    /**
     * Updates the given event state with the provided values for setpoints, door states, and creation time.
     *
     * @param state      The event state to update.
     * @param setPoint1  Current value for setpoint 1.
     * @param setPoint2  Current value for setpoint 2.
     * @param setPoint3  Current value for setpoint 3.
     * @param rearDoor   Current state of the rear door (open/closed).
     * @param sideDoor   Current state of the side door.
     * @param zone1Door  Current state of zone 1 door.
     * @param zone2Door  Current state of zone 2 door.
     * @param zone3Door  Current state of zone 3 door.
     * @param created    Timestamp representing the last update time.
     */
    public static void setOldState(EventState state, Double setPoint1, Double setPoint2, Double setPoint3,
                                   Boolean rearDoor, Boolean sideDoor, Boolean zone1Door,
                                   Boolean zone2Door, Boolean zone3Door, long created){
        state.setSetPoint1(setPoint1);
        state.setSetPoint2(setPoint2);
        state.setSetPoint3(setPoint3);
        state.setRearDoorOpen(rearDoor);
        state.setSideDoorOpen(sideDoor);
        state.setZone1DoorOpen(zone1Door);
        state.setZone2DoorOpen(zone2Door);
        state.setZone3DoorOpen(zone3Door);
        state.setLastTime(created);
    }

    /**
     * Sets the control room IDs on the provided general event based on its type, source, and metadata.
     *
     * @param generalEvent The event to update with control room IDs.
     * @param metadata     Metadata containing control room information.
     * @param source       Source identifier used for filtering control rooms.
     */
    public static void setControlRooms(GeneralEvent generalEvent, TmMetadata metadata, String source) {
        List<String> controlRoomIds = Utils.getControlRooms(generalEvent.getType(), source, metadata.getControlRooms());

        generalEvent.setControlRoomIds(controlRoomIds);
    }

    /**
     * Creates and returns a {@link Load} object populated with active load and routing violation data if available.
     * <p>
     * If no relevant data is found, this method returns {@code null}.
     *
     * @param metadata Metadata containing active load and routing violation details.
     * @return A populated Load object or {@code null} if no load data is available.
     */
    public static Load createLoad(TmMetadata metadata) {
        Load load = new Load();
        ActiveLoad activeLoad = metadata.getActiveLoad();
        RoutingViolations routingViolations = metadata.getRoutingViolations();
        boolean hasData = false;
        if (activeLoad != null) {
            load.setId(activeLoad.getLoadId());
            load.setSiteId(activeLoad.getSiteId());
            load.setReference(activeLoad.getLoadReference());
            load.setDriverName(activeLoad.getDriverName());
            load.setFleetNumber(activeLoad.getFleetNumber());
            load.setLicenceNumber(activeLoad.getLicenceNo());
            hasData = true;
        }

        if (routingViolations != null) {
            load.setUnplannedStopDuration(routingViolations.getUnplannedStop().getDurationInSeconds());
            load.setExcessStopDuration(routingViolations.getExcessStop().getDurationInSeconds());
            hasData = true;
        }

        return hasData ? load : null;
    }
}
