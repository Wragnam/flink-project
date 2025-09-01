package za.co.trackmatic.flink.flatmaps.multipletps;

import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.*;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.MultipleProviders.EventState;
import za.co.trackmatic.flink.models.MultipleProviders.RawLiveposData;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.*;

import java.io.Serializable;
import java.util.*;

/**
 * Flink {@link RichFlatMapFunction} that processes raw live position data from multiple TPS sources
 * and converts it into unified {@link GeneralEvent} instances.
 *
 * <p>This operator maintains per-device state in a keyed {@link MapState} to track
 * event-related information such as geofences entered/exited, ignition status, and other sensor states.
 * It processes raw GPS data, ignition signals, fuel levels, battery info, door states, and other telemetry
 * to generate detailed domain events used downstream.
 *
 * <p>The state uses a TTL of 10 hours to expire stale data, avoiding memory leaks for inactive devices.
 *
 * <p>The main logic includes:
 * <ul>
 *     <li>Cleaning the device serial and loading the device event state from Flink state.</li>
 *     <li>Skipping processing if the event timestamp is older than the last seen timestamp.</li>
 *     <li>Creating and populating a {@link GeneralEvent} based on the raw data and metadata.</li>
 *     <li>Checking geofence memberships and creating arrival/departure geofence events.</li>
 *     <li>Handling ignition state logic when ignition data is missing or ambiguous.</li>
 *     <li>Generating other domain-specific events such as panic, battery levels, accidents, door states, and low fuel alerts.</li>
 *     <li>Updating the internal state and storing it back to the Flink state backend.</li>
 * </ul>
 *
 * <p>This class acts as a unified event processing layer for multiple telematics providers.
 */
public class UniversalLiveposEventProcessor extends RichFlatMapFunction<RawLiveposData, GeneralEvent> implements Serializable {

    private Config config;


    public UniversalLiveposEventProcessor(Config config) {
        this.config = config;
    }

    private transient MapState<String, EventState> mapState;

    /**
     * Initializes the operator by setting up the state descriptor and configuring the time-to-live (TTL)
     * settings for the map state that holds the `EventState` for each device serial.
     * <p>
     * This method defines the map state for storing the state of each device (using the serial as the key),
     * and configures the TTL settings so that the state expires after a certain duration. The state is cleaned
     * up incrementally to avoid memory bloat.
     * </p>
     *
     * @param parameters The configuration parameters for the operator.
     * @throws Exception If an error occurs during the state initialization.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mapState = StateUtils.createMapStateWithTTL(getRuntimeContext(), config.getFlinkJobName() + "_event_map",
                Time.hours(3), TypeInformation.of(String.class), new TypeHint<EventState>() {
                },
                10000);
    }

    /**
     * Processes incoming raw live position data (`RawLiveposData`) for a device and generates
     * corresponding `GeneralEvent` instances based on the current state and geofence information.
     * <p>
     * This method performs several steps:
     * <ul>
     *   <li>Normalizes the device serial number and retrieves or initializes the event state for it.</li>
     *   <li>Ignores any data that is older or equal to the last processed event to avoid duplicates or regressions.</li>
     *   <li>Extracts positional, sensor, and status data from the raw input.</li>
     *   <li>Creates a new `GeneralEvent` populated with location, speed, direction, and metadata.</li>
     *   <li>Determines which geofences the current location falls within and updates geofence states accordingly.</li>
     *   <li>Derives ignition status from raw ignition data or infers it from speed and previous state.</li>
     *   <li>Triggers creation of geofence arrival/departure events and other event types such as panic, battery alerts, door state changes, accident detection, and fuel level warnings.</li>
     *   <li>Updates and persists the event state with the latest information.</li>
     * </ul>
     *
     * @param rawData   The raw live position data containing telemetry and sensor readings.
     * @param collector The collector used to emit generated `GeneralEvent` instances downstream.
     * @throws Exception If any error occurs during processing, such as state access or event creation.
     */
    @Override
    public void flatMap(RawLiveposData rawData, Collector<GeneralEvent> collector) throws Exception {

        // Clean the serial number and get configuration details
        String serial = Utils.cleanSerial(rawData.getSerial());
        String server = config.getDatacacheServer();

        // Retrieve the existing event state or initialize a new one for the device serial
        EventState state = new EventState();
        if (mapState.contains(serial) && mapState.get(serial) != null) state = mapState.get(serial);
        else mapState.put(serial, state);

        TmMetadata metadata = rawData.getMetadata();
        String dateString = rawData.getDateString();

        long created = rawData.getCreated();

        if (created <= state.getLastTime()) {
            return;
        }

        double lat = rawData.getLatitude();
        double lon = rawData.getLongitude();
        double speed = rawData.getSpeed();
        Double direction = rawData.getDirection();
        Double accuracy = rawData.getAccuracy();
        Boolean ignition = rawData.getIgnition();
        String dataSource = rawData.getSource();
        Double mainBattery = rawData.getMainBatteryLevel();
        Double backupBattery = rawData.getBackupBatteryLevel();
        Double setPoint1 = rawData.getSetPoint1();
        Double setPoint2 = rawData.getSetPoint2();
        Double setPoint3 = rawData.getSetPoint3();
        Boolean rearDoorOpen = rawData.getRearDoorOpen();
        Boolean sideDoorOpen = rawData.getSideDoorOpen();
        Boolean zone1DoorOpen = rawData.getZone1DoorOpen();
        Boolean zone2DoorOpen = rawData.getZone2DoorOpen();
        Boolean zone3DoorOpen = rawData.getZone3DoorOpen();
        Double fuelLevel = rawData.getFuelLevel();
        Integer fuelTankSize = rawData.getFuelTankSize();

        GeneralEvent generalEvent = new GeneralEvent(lat, lon, created,
                dataSource, metadata, serial, rawData.getDeviceId());

        generalEvent.setDischargeAir(rawData.getDischargeAir());
        generalEvent.setReturnAir(rawData.getReturnAir());
        generalEvent.setDate(Utils.convertDate(dateString, rawData.getDateFormat(), Utils.dateFormat));
        generalEvent.setSpeed(speed);
        generalEvent.setDirection(direction);
        generalEvent.setAccuracy(accuracy);

        // Check if the current location is within any geofence
        List<GeofenceItem> fences = CacheUtils.isPointInGeofences(server, metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
        Utils.setLocationNameAndId(fences, generalEvent);

        boolean ignitionStatus;
        if (ignition == null) {
            String ignitionStr = rawData.getIgnitionString();
            Boolean currentIgnition = state.isIgnitionOn();

            if (ignitionStr != null) {
                if ("IGNITION_ON".equals(ignitionStr)) {
                    state.setIgnitionOn(true);
                } else if ("IGNITION_OFF".equals(ignitionStr)) {
                    state.setIgnitionOn(false);
                } else if (currentIgnition == null) {
                    state.setIgnitionOn(speed != 0);
                }
            } else {
                if (speed > 0) {
                    state.setIgnitionOn(true);
                    state.setSpeedIgnitionCounter(0);
                } else {
                    state.incrementSpeedIgnitionCounter();

                    if (Boolean.TRUE.equals(currentIgnition) && state.getSpeedIgnitionCounter() >= 3) {
                        state.setIgnitionOn(false);
                    }
                }
            }

            ignitionStatus = Boolean.TRUE.equals(state.isIgnitionOn());
        } else {
            ignitionStatus = ignition;
        }

        // Determine geofence events based on the state and the location
        Pair<List<GeofenceToBeCreated>, Map<String, String>> geofenceData = Utils.geofenceEvent(state.getGeofences(), fences, speed, ignitionStatus, state.getCurrentArrivedAtGeofences());

        // Only if ALL current geofences are not in the planned stops will the checkUnplanned be true
        boolean checkUnplanned = Utils.checkUnplannedStop(metadata, geofenceData.getSecond());

        // Process the geofence events
        for (GeofenceToBeCreated geofenceToBeCreated : geofenceData.getFirst()) {
            EventUtils.createGeofenceEvent(generalEvent, geofenceToBeCreated, checkUnplanned, created, state, metadata, dataSource, collector);
            if (checkUnplanned && Objects.equals(geofenceToBeCreated.getType(), GeofenceEvent.SUBTYPE_ARRIVAL)) {
                break; // Stop further processing if an arrival geofence is found during an unplanned stop
            }
        }

        // Update the event state with the latest geofence information
        state.setGeofences(geofenceData.getSecond());

        // Process other event types such as panic, battery, unknown stop, and accident
        EventUtils.createUnknownStopEvent(generalEvent, ignitionStatus ? 1 : 0, collector, state, metadata, fences, dataSource);
        if (rawData.getPanic() != null)
            EventUtils.createPanicEvent(generalEvent, state, rawData.getPanic(), collector, metadata, dataSource);
        if (mainBattery != null)
            EventUtils.createMainBatteryEvent(generalEvent, mainBattery, collector, state, metadata, dataSource);
        if (backupBattery != null)
            EventUtils.createBackupBatteryEvent(generalEvent, backupBattery, collector, state, metadata, dataSource);
        if (dataSource.equals(SourceMapping.PARAGON) && rawData.getCdd() != null)
            ParagonUtils.createAccidentEvent(generalEvent, rawData.getCdd(), state, collector, metadata);
        if (setPoint1 != null || setPoint2 != null || setPoint3 != null) EventUtils.collectSetpointEvents(
                state, setPoint1, setPoint2, setPoint3, generalEvent, collector, metadata, dataSource
        );
        if (rearDoorOpen != null || sideDoorOpen != null
                || zone1DoorOpen != null || zone2DoorOpen != null
                || zone3DoorOpen != null) EventUtils.collectDoorStateEvents(
                rearDoorOpen, sideDoorOpen,
                zone1DoorOpen, zone2DoorOpen, zone3DoorOpen,
                generalEvent, state, collector, metadata, dataSource
        );
        if (fuelLevel != null && fuelTankSize != null && fuelTankSize != 0
                && (fuelLevel / fuelTankSize) <= .2 && !state.isFuelLevelLow()) EventUtils.createLowFuelEvent(
                fuelLevel, fuelTankSize, generalEvent, collector, state, metadata, dataSource
        );
        else if (fuelLevel != null && fuelTankSize != null && fuelTankSize != 0
                && (fuelLevel / fuelTankSize) > 0.2
        ) state.setFuelLevelLow(false);

        EventUtils.setOldState(state, setPoint1, setPoint2, setPoint3,
                rearDoorOpen, sideDoorOpen, zone1DoorOpen, zone2DoorOpen, zone3DoorOpen,
                created);
        mapState.put(serial, state);
    }
}
