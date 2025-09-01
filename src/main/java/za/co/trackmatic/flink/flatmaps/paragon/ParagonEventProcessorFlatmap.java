package za.co.trackmatic.flink.flatmaps.paragon;

import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.EventUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.MultipleProviders.EventState;
import za.co.trackmatic.flink.models.paragon.*;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.GeofenceToBeCreated;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.Utils.ParagonUtils;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A Flink RichFlatMapFunction that processes raw Paragon event data ({@link ParagonRawData}) and
 * transforms it into enriched {@link GeneralEvent} instances.
 * <p>
 * This processor handles multiple responsibilities including parsing GPS and sensor data, checking
 * and updating geofence states, handling special events such as panic alerts, battery status changes,
 * unknown stops, and accident detection specific to Paragon devices.
 * </p>
 * <p>
 * The class maintains per-device event state in Flink managed keyed state with a configured TTL to
 * efficiently manage state memory. It performs data cleansing, timestamp validation, and geofence event
 * determination before emitting the resulting events downstream.
 * </p>
 */
public class ParagonEventProcessorFlatmap extends RichFlatMapFunction<ParagonRawData, GeneralEvent> implements Serializable {
    private static final String dataSource = SourceMapping.PARAGON;

    private Config config;


    public ParagonEventProcessorFlatmap(Config config) {
        this.config = config;
    }

    private transient MapState<String, EventState> mapState;

    private static final String dateFormatConverted = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final String dateFormatOriginal = "yyyy-MM-dd HH:mm:ss";

    SimpleDateFormat dateParser = new SimpleDateFormat(dateFormatOriginal);

    /**
     * Initializes the operator by setting up the state descriptor and configuring the time-to-live (TTL)
     * settings for the map state that holds the `ParagonEventState` for each device serial.
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
        MapStateDescriptor<String, EventState> descriptor = new MapStateDescriptor<>("pgDeviceStates", String.class, EventState.class);

        // Configure the time-to-live (TTL) for the map state, where the state expires after 10 hours
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10000, true)
                .build();

        // Enable the TTL configuration on the state descriptor
        descriptor.enableTimeToLive(ttlConfig);

        // Get the map state from the runtime context using the configured descriptor
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    /**
     * Processes a `ParagonRawData` event, extracting and transforming relevant information to create `ParagonGeneralEvent`
     * instances, and collects these events. The method handles geofence checks, panic events, battery events, unknown
     * stops, and accidents by utilizing utility methods.
     * <p>
     * The method checks if the event data contains valid GPS information, processes the data accordingly, and creates
     * associated events based on geofence and event state conditions. It also updates the map state for each device serial
     * and stores the updated event state.
     * </p>
     *
     * @param rawEventData The raw event data to be processed, containing data items to be parsed and transformed.
     * @param collector    The collector to collect processed events.
     * @throws Exception If any exception occurs during the processing of the data.
     */
    @Override
    public void flatMap(ParagonRawData rawEventData, Collector<GeneralEvent> collector) throws Exception {
        // Return early if there is no data to process
        if (rawEventData.getData().isEmpty()) {
            return;
        }

        // Clean the serial number and get configuration details
        String serial = Utils.cleanSerial(rawEventData.getSerial());
        String server = config.getDatacacheServer();
        String orgId = rawEventData.getMetadata().getOrgId();
        String model = rawEventData.getSver();

        Logger.log("Processing Paragon event data for device: " + serial, 5556);

        // Retrieve the existing event state or initialize a new one for the device serial
        EventState state = new EventState();
        if (mapState.contains(serial) && mapState.get(serial) != null) {
            state = mapState.get(serial);
        } else {
            mapState.put(serial, state);
        }

        TmMetadata metadata = rawEventData.getMetadata();

        // Iterate through each data item in the raw event data
        for (DataItem item : rawEventData.getData()) {
            // Skip if GPS data is missing or invalid
            if (item.getGps() == null) {
                continue;
            }

            if (item.getGps().getFi() == 0) {
                continue;
            }

            String dateString = item.getGps().getDt();

            Date date = dateParser.parse(dateString);

            if (date.getTime() <= state.getLastTime()) {
                continue;
            }

            double lat;
            double lon;
            try {
                lat = Utils.parseLatitude(item.getGps().getLa());
                lon = Utils.parseLongitude(item.getGps().getLo());
            } catch (Exception e) {
                Logger.log("Parsing lat and long failed: lat -> " + item.getGps().getLa() + ", lon -> " + item.getGps().getLo(), 5556);
                lat = 0;
                lon = 0;
            }
            double speed = item.getGps().getSp();
            double direction = item.getGps().getCo();
            double accuracy = item.getGps().getNs() != null ? (item.getGps().getNs().doubleValue() / 12) * 100 : 0;

            GeneralEvent generalEvent = new GeneralEvent(lat, lon, date.getTime() / 1000,
                    dataSource, metadata, serial, serial);

            generalEvent.setDischargeAir(ParagonUtils.convertVoltageToDegreesCelsius(item.getT1(), model));
            generalEvent.setReturnAir(ParagonUtils.convertVoltageToDegreesCelsius(item.getT2(), model));
            generalEvent.setDate(Utils.convertDate(dateString, dateFormatOriginal, dateFormatConverted));
            generalEvent.setSpeed(speed);
            generalEvent.setDirection(direction);
            generalEvent.setAccuracy(accuracy);

            // Check if the current location is within any geofence
            List<GeofenceItem> fences = CacheUtils.isPointInGeofences(server, orgId, new LatLong(lat, lon), metadata.getSiteIds());
            Utils.setLocationNameAndId(fences, generalEvent);

            // Determine geofence events based on the state and the location
            Pair<List<GeofenceToBeCreated>, Map<String, String>> geofenceData = Utils.geofenceEvent(state.getGeofences(), fences, item.getGps().getSp(), item.getIg() == 1);

            // Only if ALL current geofences are not in the planned stops will the checkUnplanned be true
            boolean checkUnplanned = Utils.checkUnplannedStop(rawEventData.getMetadata(), geofenceData.getSecond());

            // Process the geofence events
            for (GeofenceToBeCreated geofenceToBeCreated : geofenceData.getFirst()) {
                EventUtils.createGeofenceEvent(generalEvent, geofenceToBeCreated, checkUnplanned, date, state, metadata, dataSource, collector);
                if (checkUnplanned && Objects.equals(geofenceToBeCreated.getType(), "ARRIVAL")) {
                    break; // Stop further processing if an arrival geofence is found during an unplanned stop
                }
            }

            // Update the event state with the latest geofence information
            state.setGeofences(geofenceData.getSecond());

            Double A1 = item.getAdc() != null ? item.getAdc().getA1() : null;
            Double A2 = item.getAdc() != null ? item.getAdc().getA2() : null;

            // Process other event types such as panic, battery, unknown stop, and accident
            ParagonUtils.createPanicEvent(generalEvent, state, item.getPa(), collector, metadata);
            ParagonUtils.createBatteryEvent(generalEvent, A1, A2, collector, state, metadata);
            EventUtils.createUnknownStopEvent(generalEvent, item.getIg(), collector, state, metadata, fences, dataSource);
            ParagonUtils.createAccidentEvent(generalEvent, item.getCdd(), state, collector, metadata);

            // Store the updated event state for the device serial
            state.setLastTime(date.getTime());
            mapState.put(serial, state);
        }
    }
}
