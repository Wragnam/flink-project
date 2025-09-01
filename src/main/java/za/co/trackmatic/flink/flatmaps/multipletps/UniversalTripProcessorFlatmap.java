package za.co.trackmatic.flink.flatmaps.multipletps;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.TripProcessingUtils;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.RawTripData;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitRequest;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitResponse;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.trips.TripEventMessage;
import za.co.trackmatic.flink.models.trips.TripGeoMessage;
import za.co.trackmatic.flink.models.trips.TripGeoSnapshot;
import za.co.trackmatic.flink.models.trips.TripTopicResponse;


import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Flink RichFlatMapFunction that processes raw trip data streams and produces enriched trip topic responses.
 * <p>
 * This class maintains a keyed state per device serial number to track trip geometry and event messages,
 * supports trip lifecycle management (start, ongoing, end), and computes trip statuses using speed limits and ignition data.
 * It also manages time-to-live (TTL) configuration for state entries to optimize resource usage.
 * </p>
 */
public class UniversalTripProcessorFlatmap extends RichFlatMapFunction<RawTripData, TripTopicResponse> implements Serializable {
    private Config config;

    public UniversalTripProcessorFlatmap(Config config) {
        this.config = config;
    }

    private transient MapState<String, Tuple2<TripGeoMessage, TripEventMessage>> mapState;


    /**
     * Initializes the operator with a map state for tracking trip messages and configures a Time-to-Live (TTL)
     * policy for the state entries. This setup helps manage memory by allowing entries to expire after a specified time.
     *
     * <p>The map state (`mapState`) holds a mapping between device serial numbers and tuples containing trip messages
     * ({@link TripGeoMessage} and {@link TripEventMessage}). The state is configured with a TTL of 5 hours, meaning entries
     * older than 5 hours will be expired and removed incrementally by the Flink runtime.</p>
     *
     * @param parameters Configuration parameters provided by the Flink runtime for initializing state.
     * @throws Exception if an error occurs during the initialization of the map state.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // Call the parent class's open method to ensure any necessary setup is completed.
        super.open(parameters);

        // Configure Time-to-Live (TTL) settings for the state, allowing the state to expire after a specified duration.
        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.hours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // Enable incremental cleanup to run garbage collection of expired state entries periodically.
                .cleanupIncrementally(10000, true)
                .build();

        // Define a MapState descriptor to hold mapping between serial numbers and trip data messages.
        // This map state will store the TripGeoMessage and TripEventMessage tuples for each serial (device ID).
        MapStateDescriptor<String, Tuple2<TripGeoMessage, TripEventMessage>> descriptor = new MapStateDescriptor<>(config.getFlinkJobName() + "_TripMap",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<TripGeoMessage, TripEventMessage>>() {
                }));
        descriptor.enableTimeToLive(stateTtlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }


    /**
     * Processes incoming {@link RawTripData} records, updates trip state, and emits {@link TripTopicResponse} events.
     * <p>
     * This method implements trip start, continuation, and ending logic using speed, ignition status,
     * and cached trip state. It also enriches events with speed limits, geolocation snapshots, and metadata.
     * The trip and event messages are stored in keyed state with an expiration TTL.
     * </p>
     *
     * @param rawTripData The raw trip data input event containing location, speed, ignition, and metadata.
     * @param collector   The collector used to emit {@link TripTopicResponse} events downstream.
     * @throws Exception If any processing or state update operation fails.
     */
    @Override
    public void flatMap(RawTripData rawTripData, Collector<TripTopicResponse> collector) throws Exception {
        // Clean and standardize the serial identifier of the vehicle.
        String serial = Utils.cleanSerial(rawTripData.getSerial());

        // Retrieve or initialize the trip and event messages from the state for the given serial.
        TripGeoMessage tripGeoMessage;
        TripEventMessage tripEventMessage;
        if (mapState.contains(serial)) {
            Tuple2<TripGeoMessage, TripEventMessage> tuple = mapState.get(serial);
            tripGeoMessage = tuple.f0;
            tripEventMessage = tuple.f1;
        } else {
            tripGeoMessage = new TripGeoMessage();
            tripEventMessage = new TripEventMessage();
        }

        double lat = rawTripData.getLatitude();
        double lon = rawTripData.getLongitude();
        double speed = rawTripData.getSpeed();
        String date = rawTripData.getDateString();
        Double direction = rawTripData.getDirection();
        TmMetadata metadata = rawTripData.getMetadata();
        long timeInSeconds = rawTripData.getUtcTimeValue();
        String dateFormatOriginal = rawTripData.getDateFormat();
        Boolean ignition = rawTripData.getIgnition();

        boolean noIgnition = ignition == null && rawTripData.getIgnitionString() == null;

        String server = config.getDatacacheServer();

        SpeedLimitResponse speedLimit = Utils.getSpeedLimit(new SpeedLimitRequest(new LatLong(lat, lon)));

        String status = TripProcessingUtils.getStatus(speed, speedLimit);

        boolean isEnd = false;
        //Check if this is start of a new trip
        if (tripGeoMessage.getLastTripId() == null && (noIgnition ? (speed > 5 && !tripGeoMessage.isActiveTrip())
                : ((ignition == null ? rawTripData.getIgnitionString().equals("IGNITION_ON") : ignition) || speed > 5))) {
            long tId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            tripGeoMessage.setLastTripId(tId);

            if (noIgnition) tripGeoMessage.setActiveTrip(true);
            tripGeoMessage.setLastSeq(0);

            TripProcessingUtils.setInitialTripMessage(tripGeoMessage, tripEventMessage, metadata, serial, tId);

            tripGeoMessage.setSource(rawTripData.getSource());

            tripGeoMessage.setStartEvent(TripProcessingUtils.setStartAndEndEvent(metadata.getOrgId(), metadata.getSiteIds(), lat, lon, server, date, dateFormatOriginal));
            TripProcessingUtils.setPreviousDetails(tripGeoMessage, lat, lon, timeInSeconds);

            tripGeoMessage.setId(UUID.randomUUID() + "_" + tId);

            TripProcessingUtils.setEventData(tripEventMessage, speedLimit, status, lat, lon, server, metadata, tripGeoMessage, date, dateFormatOriginal, speed);
        }
        // If we're continuing an existing trip, process the data and update sequence.
        else if (tripGeoMessage.getLastTripId() != null && (noIgnition ? tripGeoMessage.isActiveTrip() :
                ignition == null ? !rawTripData.getIgnitionString().equals("IGNITION_OFF") : ignition)) {
            if (tripGeoMessage.getLastTime() < timeInSeconds) { //Make sure trip messages are always the next message and not any prior messages
                TripGeoSnapshot tripGeoSnapshot = TripProcessingUtils.createGeoSnapshot(lat, lon, metadata.getOrgId(), metadata.getSiteIds(), speed, status, rawTripData.getAccuracy(), date, dateFormatOriginal, direction, config.getDatacacheServer(), rawTripData.getSatNum(), rawTripData.getBatteryLevel());
                tripGeoMessage.getSnapshots().add(tripGeoSnapshot);

                TripProcessingUtils.updateComputedValues(tripGeoMessage, speed, timeInSeconds, status, lat, lon, true);
                TripProcessingUtils.setPreviousDetails(tripGeoMessage, lat, lon, timeInSeconds);
                TripProcessingUtils.setEventData(tripEventMessage, speedLimit, status, lat, lon, server, metadata, tripGeoMessage, date, dateFormatOriginal, speed);

                if (noIgnition) {
                    Integer lastSeq = tripGeoMessage.getLastSeq();
                    if (speed == 0) tripGeoMessage.setLastSeq((lastSeq != null ? lastSeq : 0) + 1);
                    else tripGeoMessage.setLastSeq(0);
                    if (tripGeoMessage.getLastSeq() != null && tripGeoMessage.getLastSeq() >= 3)
                        tripGeoMessage.setActiveTrip(false);
                }

            }
        }
        // Handle trip ending conditions when ignition is off and priority flag is set.
        else if ((noIgnition ? !tripGeoMessage.isActiveTrip() :
                ignition == null ? rawTripData.getIgnitionString().equals("IGNITION_OFF") : !ignition)
                && tripGeoMessage.getLastTripId() != null) {//Trip ends
            TripProcessingUtils.collectTrip(tripGeoMessage, lat, lon, status, tripEventMessage, speedLimit, serial, collector, metadata, serial, date, dateFormatOriginal, speed);
            tripGeoMessage = new TripGeoMessage();
            tripEventMessage = new TripEventMessage();
            isEnd = true;
        }

        // Update the last known status of the trip event.
        if (!isEnd) {
            tripEventMessage.setLastStatus(status);
        }

        // Store the updated trip and event message in the map state for this serial.
        Tuple2<TripGeoMessage, TripEventMessage> t = new Tuple2<>();
        t.f0 = tripGeoMessage;
        t.f1 = tripEventMessage;
        mapState.put(serial, t);
    }
}
