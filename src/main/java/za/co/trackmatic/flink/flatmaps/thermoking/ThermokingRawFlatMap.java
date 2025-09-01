//package za.co.trackmatic.flink.flatmaps.thermoking;
//
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.util.Collector;
//import za.co.trackmatic.flink.CacheUtils;
//import za.co.trackmatic.flink.Utils.EventUtils;
//import za.co.trackmatic.flink.Utils.SourceMapping;
//import za.co.trackmatic.flink.Utils.Utils;
//import za.co.trackmatic.flink.events.GeneralEvent;
//import za.co.trackmatic.flink.events.thermoking.*;
//import za.co.trackmatic.flink.models.Config;
//import za.co.trackmatic.flink.models.MultipleProviders.DoorStateEvent;
//import za.co.trackmatic.flink.models.MultipleProviders.LowFuelEvent;
//import za.co.trackmatic.flink.models.MultipleProviders.SetPointEvent;
//import za.co.trackmatic.flink.models.thermoking.ThermokingData;
//import za.co.trackmatic.flink.models.trackmatic.GeofenceEvent;
//import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
//import za.co.trackmatic.flink.models.trackmatic.LatLong;
//import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
//import za.co.trackmatic.flink.thermoking.CacheManager;
//import za.co.trackmatic.flink.thermoking.DeviceState;
//import za.co.trackmatic.flink.thermoking.GeofenceManager;
//import za.co.trackmatic.flink.thermoking.StateAndEvents;
//
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
//public class ThermokingRawFlatMap extends RichFlatMapFunction<ThermokingData, GeneralEvent> implements Serializable {
//
//    /**
//     * A map of device state. The key is the sanitized reeferSerialNumber.
//     */
////    private transient MapState<String, DeviceState> mapState;
//
//    private static class ThermokingMap {
//
//        public ThermokingMap() {
//            this.deviceState = new DeviceState();
//            this.geofences = new HashMap<>();
//        }
//
//        private DeviceState deviceState;
//
//        private Map<String, String> geofences;
//
//        public DeviceState getDeviceState() {
//            return deviceState;
//        }
//
//        public void setDeviceState(DeviceState deviceState) {
//            this.deviceState = deviceState;
//        }
//
//        public Map<String, String> getGeofences() {
//            return geofences;
//        }
//
//        public void setGeofences(Map<String, String> geofences) {
//            this.geofences = geofences;
//        }
//    }
//
//    private transient MapState<String, ThermokingMap> mapState;
//
//    private static final String dataSource = SourceMapping.THERMOKING;
//
//
//    private Config config;
//
//    public ThermokingRawFlatMap(Config config) {
//        this.config = config;
//    }
//
//    /**
//     * Processes the state of a device by evaluating its geofences, caching data, and updating its state and associated events.
//     * <p>
//     * This method handles the processing of device state information by utilizing a `GeofenceManager` to check geofence events
//     * and a `CacheManager` to update the state and events based on the raw data. The method returns a `StateAndEvents` object
//     * that contains the updated device state and any events triggered during the processing.
//     * </p>
//     *
//     * @param server    The server address to be used for geofence processing.
//     * @param serial    The serial number of the device.
//     * @param orgId     The organization ID associated with the device.
//     * @param rawData   The raw Thermoking data to be processed.
//     * @param state     The current state of the device.
//     * @param geofences A map of geofences associated with the device.
//     * @return The updated `StateAndEvents` object containing the processed device state and events.
//     */
//    private StateAndEvents processDeviceState(String server, String serial, String orgId, ThermokingData rawData, DeviceState state, Map<String, String> geofences) {
//        // Create a new StateAndEvents object to store the processed state and events
//        StateAndEvents stateAndEvents = new StateAndEvents();
//        stateAndEvents.setDeviceState(state);
//
//        // Initialize the GeofenceManager to process geofences
//        GeofenceManager gm = new GeofenceManager();
//
//        // Process geofences using the GeofenceManager and update the state and events
//        stateAndEvents = gm.processGeofences(server, orgId, rawData, stateAndEvents, geofences);
//
//        // Initialize the CacheManager to handle state and event caching
//        CacheManager cm = new CacheManager();
//
//        // Process the raw data and update the state and events using the CacheManager
//        stateAndEvents = cm.process(serial, rawData, stateAndEvents);
//
//        // Return the updated StateAndEvents object
//        return stateAndEvents;
//    }
//
//    /**
//     * Converts a given event to a `TpsEvent` by mapping relevant data from the raw Thermoking data and the event type.
//     * <p>
//     * This method creates a `TpsEvent` and populates it with information extracted from the provided `rawData` and `event`
//     * based on the specified event type (`evType`). It sets attributes like asset ID, organization ID, creation time, event
//     * type, and geographic location.
//     * </p>
//     *
//     * @param event   The event to be converted into a `TpsEvent`.
//     * @param rawData The raw Thermoking data containing metadata and other device information.
//     * @param evType  The type of event (e.g., "door", "fuel", "setpoint", "geofence") to determine the event type.
//     * @return The corresponding `TpsEvent` object populated with relevant data from the event and raw data.
//     */
//    private GeneralEvent convertEventToGeneralEvent(Event event, ThermokingData rawData, String evType, GeofenceItem geofenceItem) {
//
//        String deviceId = rawData.getReeferSerialNumber().trim();
//        // Create a new GeneralEvent to populate with data
//        GeneralEvent generalEvent = new GeneralEvent(rawData.getLatitude(), rawData.getLongitude(),
//                Utils.getCreatedTimeFromThermokingData(rawData), dataSource, rawData.getTmMetadata(), deviceId, deviceId);
//        generalEvent.setSpeed(rawData.getSpeed());
//        generalEvent.setIgnition(Objects.equals(rawData.getIgnitionStatus(), "On"));
//
//        Utils.setLocationNameAndId(geofenceItem, generalEvent);
//
//
//        // Set the event type based on the provided event type (evType)
//        switch (evType) {
//            case "door": {
//                generalEvent.setType(DoorStateEvent.TYPE);
//                generalEvent.setDoorStateEvent(event.getDoorStateEvent());
//            }
//            break;
//            case "fuel": {
//                generalEvent.setType(LowFuelEvent.TYPE);
//                generalEvent.setLowFuelEvent(event.getLowFuelEvent());
//            }
//            break;
//            case "setpoint": {
//                generalEvent.setType(SetPointEvent.TYPE);
//                generalEvent.setSetPointEvent(event.getSetPointEvent());
//            }
//            break;
//            case "geofence": {
//                generalEvent.setType(GeofenceEvent.TYPE);
//                generalEvent.setGeofenceEvent(event.getGeofenceEvent());
//            }
//        }
//
//        // Return the fully populated TpsEvent
//        return generalEvent;
//    }
//
//    /**
//     * Initializes the state for the operator by setting up a MapState with a time-to-live (TTL) configuration.
//     * <p>
//     * This method is called when the operator is initialized and sets up the state descriptor, TTL configuration,
//     * and the state object. The MapState stores a mapping between device serial numbers (as Strings) and
//     * corresponding `ThermokingMap` objects. The TTL configuration ensures that the state will be cleaned up after
//     * 5 hours of inactivity, and the state is never returned once expired.
//     * </p>
//     *
//     * @param parameters The configuration parameters to be passed during operator initialization.
//     * @throws Exception If an error occurs during initialization.
//     */
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        MapStateDescriptor<String, ThermokingMap> descriptor = new MapStateDescriptor<>("orgDeviceStates", String.class, ThermokingMap.class);
//
//        // Configure the time-to-live (TTL) settings for the state
//        StateTtlConfig ttlConfig = StateTtlConfig
//                .newBuilder(Time.hours(5))
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .cleanupIncrementally(5000, true)
//                .build();
//
//        // Enable TTL for the state descriptor
//        descriptor.enableTimeToLive(ttlConfig);
//
//        // Initialize the mapState with the descriptor
//        mapState = getRuntimeContext().getMapState(descriptor);
//    }
//
//    /**
//     * Processes Thermoking data and converts it into TpsEvent objects, which are then collected for further processing.
//     * <p>
//     * This method processes the input Thermoking data, retrieves or creates the corresponding state for the device,
//     * and processes geofences and events. The resulting events are then converted into TpsEvent objects, and additional
//     * metadata is added before the events are emitted to the collector.
//     * </p>
//     *
//     * @param thermokingData The input Thermoking data object that contains information about the reefer device.
//     * @param collector      The collector used to emit the resulting TpsEvent objects.
//     * @throws Exception If an error occurs during processing.
//     */
//    @Override
//    public void flatMap(ThermokingData thermokingData, Collector<GeneralEvent> collector) throws Exception {
//        // Clean the serial number from the input data
//        String serial = Utils.cleanSerial(thermokingData.getReeferSerialNumber());
//        if (serial.equals(Utils.UNKNOWN)) {
//            return; // Skip processing if the serial number is unknown
//        }
//
//        // Retrieve the ThermokingMap from the state or create a new one if not found
//        ThermokingMap map;
//        if (mapState.contains(serial)) {
//            map = mapState.get(serial);
//        } else {
//            map = new ThermokingMap();
//            mapState.put(serial, map);
//        }
//
//        TmMetadata metadata = thermokingData.getTmMetadata();
//
//        // Retrieve the organization ID from the metadata
//        String orgId = metadata.getOrgId();
//
//        // Process the device state and events for the given device serial and Thermoking data
//        StateAndEvents stateAndEvents = processDeviceState(config.getDatacacheServer(), serial, orgId, thermokingData, map.getDeviceState(), map.getGeofences());
//
//        // Update the device state in the map
//        map.setDeviceState(stateAndEvents.getDeviceState());
//
//        GeofenceItem geofenceItem = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(),
//                new LatLong(thermokingData.getLatitude(), thermokingData.getLongitude()), metadata.getSiteIds());
//
//        // Store the updated map in the state
//        mapState.put(serial, map);
//
//        //Logger.info("event: generating " + stateAndEvents.getEvents().size() + " events");
//        // Emit the generated events as TpsEvent objects
//        for (Event event : stateAndEvents.getEvents()) {
//            // Determine the event type based on the available event fields
//            String evType = "";
//            if (event.getDoorStateEvent() != null) {
//                evType = "door";
//            }
//            if (event.getLowFuelEvent() != null) {
//                evType = "fuel";
//            }
//            if (event.getSetPointEvent() != null) {
//                evType = "setpoint";
//            }
//            if (event.getGeofenceEvent() != null) {
//                evType = "geofence";
//            }
//
//            // Convert the event to a TpsEvent
//            GeneralEvent generalEvent = convertEventToGeneralEvent(event, thermokingData, evType, geofenceItem);
//
//            EventUtils.setControlRooms(generalEvent, metadata, dataSource);
//
//            // Collect the TpsEvent for further processing
//            collector.collect(generalEvent);
//        }
//    }
//}
