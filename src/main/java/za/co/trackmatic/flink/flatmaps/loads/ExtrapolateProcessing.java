package za.co.trackmatic.flink.flatmaps.loads;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.loads.extrapolation.ExtrapolationReqAndRespEvent;
import za.co.trackmatic.flink.models.loads.extrapolation.NextStopInformation;
import za.co.trackmatic.flink.models.loads.extrapolation.ParagonLivePosData;
import za.co.trackmatic.flink.models.loads.extrapolation.UpdateLoadLoadGeoPoint;
import za.co.trackmatic.flink.models.mappingAPI.DistanceAndTimeReqBody;
import za.co.trackmatic.flink.models.mappingAPI.DistanceAndTimeResp;
import za.co.trackmatic.flink.models.mappingAPI.MappingConfig;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.Stop;
import za.co.trackmatic.flink.models.trackmatic.TmDevices;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;

/**
 * A Flink {@link RichCoFlatMapFunction} that processes live position data
 * and extrapolation request/response events for fleet tracking.
 *
 * <p>This class maintains state for each asset to track the latest GPS data and
 * extrapolation information. It computes estimated distance and travel time
 * to the next stop using mapping services and emits updated extrapolation events.
 *
 * <p>The state has a time-to-live of 10 hours to clean up inactive assets.
 */
public class ExtrapolateProcessing extends RichCoFlatMapFunction<ParagonLivePosData, ExtrapolationReqAndRespEvent, ExtrapolationReqAndRespEvent> implements Serializable {


    private Config config;
    public ExtrapolateProcessing(Config config){this.config = config;}

    /**
     * Inner state tracker for a single asset, holding the last update time,
     * current GPS data, and the latest extrapolation event.
     */
    public static class Tracker {
        public Tracker() {
            this.lastTime = 0;
        }

        private long lastTime;

        private ParagonLivePosData.Gps gps;

        private ExtrapolationReqAndRespEvent extrapolationReqAndRespEvent;

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }


        public ParagonLivePosData.Gps getGps() {
            return gps;
        }

        public void setGps(ParagonLivePosData.Gps gps) {
            this.gps = gps;
        }

        public ExtrapolationReqAndRespEvent getExtrapolationReqAndRespEvent() {
            return extrapolationReqAndRespEvent;
        }

        public void setExtrapolationReqAndRespEvent(ExtrapolationReqAndRespEvent extrapolationReqAndRespEvent) {
            this.extrapolationReqAndRespEvent = extrapolationReqAndRespEvent;
        }
    }

    SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private transient MapState<String, Tracker> tracker;

    /**
     * Initializes the MapState for tracking data with a time-to-live (TTL) configuration.
     * Sets up a MapState descriptor with a TTL of 10 hours, enabling incremental cleanup.
     *
     * @param parameters The configuration parameters passed to the function.
     * @throws Exception If an error occurs during state initialization.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Tracker> trackerDescriptor = new MapStateDescriptor<String, Tracker>("paragonTrackerState", String.class, Tracker.class);

        // Configure the state TTL to expire entries after 10 hours
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(10)).cleanupIncrementally(10, true).build();
        trackerDescriptor.enableTimeToLive(ttlConfig);

        // Initialize the tracker MapState with the descriptor in the runtime context
        tracker = getRuntimeContext().getMapState(trackerDescriptor);
    }

    /**
     * Creates a `DistanceAndTimeReqBody` payload object for mapping distance and time between
     * the tracker's current GPS location and the next stop location.
     *
     * @param nextStopInformation Contains information about the next stop, including its position and organization ID.
     * @param t The `Tracker` instance providing the current GPS location.
     * @return A `DistanceAndTimeReqBody` object configured with the start and end locations and mapping configuration.
     */
    private DistanceAndTimeReqBody getMappingPayload(NextStopInformation nextStopInformation, Tracker t){
        UpdateLoadLoadGeoPoint nextStopPos = nextStopInformation.getPosition();

        // Initialize a new payload object for the mapping request
        DistanceAndTimeReqBody payload = new DistanceAndTimeReqBody();

        // Set the starting point using the current GPS coordinates from the Tracker
        LatLong start = new LatLong(t.getGps().getLat(), t.getGps().getLng());

        // Set the ending point using the next stop coordinates from NextStopInformation
        LatLong end = new LatLong(nextStopPos.getLat(), nextStopPos.getLng());

        // Configure the payload with the start and end locations
        payload.setStart(start);
        payload.setEnd(end);

        // Create and set a mapping configuration using the organization ID from NextStopInformation
        MappingConfig mappingConfig = new MappingConfig(nextStopInformation.getOrgId());

        payload.setConfig(mappingConfig);

        // Return the fully configured payload object
        return payload;
    }

    /**
     * Checks if a specified stop has been arrived at based on the stop ID and device serial number.
     *
     * @param stopId The ID of the stop to check for arrival status.
     * @param serial The serial number of the device to retrieve from the cache.
     * @return `true` if the stop has been arrived at (arrival time is recorded), `false` otherwise.
     */
    private boolean checkIfStopHasBeenArrivedAt(String stopId, String serial){
        // Retrieve the device information from the cache using the provided serial number
        TmDevices.TmDevice device = CacheUtils.getDeviceFromCache(config.getDatacacheServer(), serial);
        if(device == null){
            return false;
        }


        // Return false if there is no active load or stops associated with the device
        if(device.getTpsActiveLoad() == null){
            return false;
        }

        List<Stop> activeLoadStops = device.getTpsActiveLoad().getStops();

        if(activeLoadStops == null){
            return false;
        }

        // Iterate through the list of stops to find the specified stop by ID
        for (Stop stop : activeLoadStops) {
            if (Objects.equals(stop.getStopId(), stopId)) {
                // Return true if the arrival time for the stop is not null, indicating arrival
                return stop.getArrival() != null;
            }
        }

        // Return false if the specified stop ID was not found or has no recorded arrival time
        return false;
    }

    /**
     * Processes extrapolation request data to estimate distance and travel time for a given asset.
     *
     * @param reqData The request event containing payload data related to extrapolation.
     * @param t       The tracker object associated with the asset, holding GPS and timing data.
     * @param checkTime A flag indicating whether to enforce a time-based update condition.
     * @return The updated {@code ExtrapolationReqAndRespEvent} with estimated distance and travel time,
     *         or {@code null} if conditions for processing are not met.
     * @throws Exception if an error occurs during distance and time calculation.
     */
    public ExtrapolationReqAndRespEvent processing(ExtrapolationReqAndRespEvent reqData, Tracker t, boolean checkTime) throws Exception {
        long currentTime = System.currentTimeMillis() / 1000;
        String assetId = reqData.getPayload().getAssetId();

        // Proceed with processing if the elapsed time since the last update is >= 300 seconds or if checkTime is false
        if (currentTime - t.getLastTime() >= 300 || !checkTime) {

            // Prepare the payload for distance and time mapping
            DistanceAndTimeReqBody mappingPayload = getMappingPayload(reqData.getPayload(), t);

            // Call utility method to retrieve distance and time response
            DistanceAndTimeResp mappingResp = Utils.getDistanceAndTime(mappingPayload);
            Logger.log("Distance and time response is: " + mappingResp, 5558);


            if(mappingResp == null){
                Logger.log("Response is null for device: " + assetId, 5558);
                return null;
            }

            // Update request payload with estimated data and other meta information
            reqData.getPayload().setCreated("");
            reqData.getPayload().setEstimatedDistance((double) mappingResp.getDistanceInMetres() /1000);
            reqData.getPayload().setEstimatedTravelTime(mappingResp.getTimeInSeconds());
            reqData.getPayload().setEventTime(t.getGps().getDateTime());
            reqData.getMeta().setType("load_extrapolation_result_event");
            reqData.getMeta().setApp("tps");

            // Update the tracker's last processed time to the current time
            t.setLastTime(currentTime);

            // Store the updated tracker information for the asset in the tracker map
            tracker.put(assetId, t);

            // Return the updated request data with estimated travel information
            return reqData;
        } else {

            // Skip processing if conditions are not met and return null
            return null;
        }
    }

    /**
     * Processes live position data for a given asset, updates tracking information, and generates
     * extrapolation response events if conditions are met.
     *
     * @param paragonLiveposEvent The live position data event for the asset.
     * @param collector           The collector used to emit the extrapolation response event.
     * @throws Exception if an error occurs during parsing or extrapolation processing.
     */
    @Override
    public void flatMap1(ParagonLivePosData paragonLiveposEvent, Collector<ExtrapolationReqAndRespEvent> collector) throws Exception {
        String assetId = paragonLiveposEvent.getAssetId();
        ParagonLivePosData.Gps gps = paragonLiveposEvent.getGps();

        // Initialize or retrieve an existing tracker for the asset
        Tracker t = new Tracker();
        if (tracker.contains(assetId)) {
            t = tracker.get(assetId);
        } else {
            // If tracker is new, set initial GPS data and add to tracker map, then exit
            t.setGps(gps);
            tracker.put(assetId, t);
            return;
        }

        // Parse timestamps for current and previous GPS data
        long time = dateParser.parse(gps.getDateTime()).getTime() / 1000;
        long previousTime = dateParser.parse(t.getGps().getDateTime()).getTime() / 1000;

        // Update tracker GPS data if the current timestamp is more recent
        if (time > previousTime) {
            t.setGps(gps);
        }

        // Store the updated tracker in the tracker map
        tracker.put(paragonLiveposEvent.getAssetId(), t);

        // If no extrapolation data exists in the tracker, exit without further processing
        if (t.getExtrapolationReqAndRespEvent() == null) {
            return;
        }

        // Check if the stop has already been reached
        boolean check = checkIfStopHasBeenArrivedAt(t.getExtrapolationReqAndRespEvent().getPayload().getStopId(), paragonLiveposEvent.getDeviceId());
        if(check){
            // Clear extrapolation event if the stop was reached and update the tracker
            t.setExtrapolationReqAndRespEvent(null);
            tracker.put(paragonLiveposEvent.getAssetId(), t);
            return;
        }

        // Process extrapolation request and response event for the tracker
        ExtrapolationReqAndRespEvent resp = processing(t.getExtrapolationReqAndRespEvent(), t, true);
        if(resp == null){
            return;
        }

        // Collect the processed extrapolation response event for further handling
        collector.collect(resp);
    }

    /**
     * Processes an extrapolation request event by updating the tracker with new extrapolation data,
     * invoking the extrapolation processing, and emitting a response if applicable.
     *
     * @param reqData   The extrapolation request and response event data.
     * @param collector The collector used to emit the extrapolated response event.
     * @throws Exception if an error occurs during extrapolation processing.
     */
    @Override
    public void flatMap2(ExtrapolationReqAndRespEvent reqData, Collector<ExtrapolationReqAndRespEvent> collector) throws Exception {
        String assetId = reqData.getPayload().getAssetId();

        // Initialize the tracker for the asset if it exists in the tracker map
        Tracker t;
        if (tracker.contains(assetId)) {
            t = tracker.get(assetId);
        } else {
            // Exit if no tracker exists for the specified asset ID
            return;
        }

        // Exit if the tracker's GPS data is missing
        if (t.getGps() == null) {
            Logger.log("Tracker's gps is empty", 5558);
            return;
        }

        // Update tracker with the current extrapolation event
        t.setExtrapolationReqAndRespEvent(reqData);

        // Process extrapolation request and generate a response
        ExtrapolationReqAndRespEvent resp = processing(reqData, t, false);

        // Update tracker in the tracker map with latest data
        tracker.put(assetId, t);

        // Return if the response is null and exit without emitting an event
        if(resp==null){
            Logger.log("Returned response is empty in Flatmap 2 for device: " + assetId, 5558);
            return;
        }

        // Collect the processed response event for further handling
        collector.collect(resp);
    }
}
