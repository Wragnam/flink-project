package za.co.trackmatic.flink.Utils;

import org.apache.commons.math3.util.Pair;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.EventMapping;
import za.co.trackmatic.flink.models.EventMappingManager;
import za.co.trackmatic.flink.models.KeyValuePair;
import za.co.trackmatic.flink.models.MultipleProviders.TempEventParameters;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureEvent;
import za.co.trackmatic.flink.models.mappingAPI.DistanceAndTimeReqBody;
import za.co.trackmatic.flink.models.mappingAPI.DistanceAndTimeResp;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.models.trackmatic.*;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitResponse;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Utility class containing various helper methods used across different components of the system.
 */
public class Utils {

    private static final String mappingURL = "https://mapping.trackmatic.co.za";
    public static final String dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * Creates a Kafka source.
     *
     * @param source       the configuration for the Kafka source
     * @param deserializer the deserialization schema for Kafka records
     * @param <T>          the type of the data to be processed
     * @return a KafkaSource instance
     */
    public static <T> KafkaSource<T> createKafkaSource(Config.KafkaSourceSink source, KafkaRecordDeserializationSchema<T> deserializer) {
        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(source.getBootstrapServers())
                .setTopics(source.getTopics())
                .setGroupId(source.getGroupId())
                .setClientIdPrefix(UUID.randomUUID().toString())
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(deserializer);

        if (source.getProperties() != null) {
            for (KeyValuePair kv : source.getProperties()) {
                builder = builder.setProperty(kv.getK(), kv.getV());
            }
        }

        // Return the built Kafka source
        return builder.build();
    }

    /**
     * Creates a Kafka source based on a specific index from a comma-separated list of sources.
     *
     * @param numSource    the index of the source to use
     * @param source       the configuration for the Kafka source
     * @param deserializer the deserialization schema for Kafka records
     * @param <T>          the type of the data to be processed
     * @return a KafkaSource instance
     */
    public static <T> KafkaSource<T> createKafkaSource(Integer numSource, Config.KafkaSourceSink source, KafkaRecordDeserializationSchema<T> deserializer) {
        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(source.getBootstrapServers())
                .setTopics(source.getTopics().split(",")[numSource].trim())
                .setGroupId(source.getGroupId().split(",")[numSource].trim())
                .setClientIdPrefix(UUID.randomUUID().toString())
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(deserializer);

        if (source.getProperties() != null) {
            for (KeyValuePair kv : source.getProperties()) {
                builder = builder.setProperty(kv.getK(), kv.getV());
            }
        }

        // Return the built Kafka source
        return builder.build();
    }

    /**
     * Creates a Kafka sink.
     *
     * @param sink       the configuration for the Kafka sink
     * @param serializer the serialization schema for Kafka records
     * @param <T>        the type of the data to be processed
     * @return a KafkaSink instance
     */
    public static <T> KafkaSink<T> createKafkaSink(Config.KafkaSourceSink sink, KafkaRecordSerializationSchema<T> serializer) {
        KafkaSinkBuilder<T> builder = KafkaSink.<T>builder()
                .setBootstrapServers(sink.getBootstrapServers())
                .setRecordSerializer(serializer);

        if (sink.getProperties() != null) {
            for (KeyValuePair kv : sink.getProperties()) {
                builder.setProperty(kv.getK(), kv.getV());
            }
        }

        // Return the built Kafka sink
        return builder.build();
    }

    public static final String UNKNOWN = "[unknown]";

    /**
     * returns a trimmed version of serial, or "[unknown]" if serial is null/empty
     *
     * @param serial the serial number
     * @return the cleaned up version
     */
    public static String cleanSerial(String serial) {
        if (serial == null || serial.isEmpty()) {
            return UNKNOWN;
        }
        return serial.trim();
    }

    /**
     * Extracts the creation time from ThermoKing data and returns it as a Unix timestamp.
     *
     * @param data the ThermoKing data
     * @return the creation time in seconds since the Unix epoch, or -1 if parsing fails
     */
    public static long getCreatedTimeFromThermokingData(ThermokingData data) {
        if (data == null || data.getDataDate() == null) {
            return -1;
        }

        try {
            ZonedDateTime zdt = ZonedDateTime.parse(data.getDataDate(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            // Convert to seconds
            return zdt.toInstant().toEpochMilli() / 1000;
        } catch (Exception e) {
            // Return -1 if parsing fails
            return -1;
        }
    }

    /**
     * Sets the location ID and name on the given {@link GeneralEvent} using the first geofence in the list.
     * If the list is null or empty, no changes are made.
     *
     * @param geofences    The list of geofence items, from which the first item is used.
     * @param generalEvent The GeneralEvent to update with location information.
     */
    public static void setLocationNameAndId(List<GeofenceItem> geofences, GeneralEvent generalEvent) {
        if (geofences != null && !geofences.isEmpty()) {
            GeofenceItem geofenceItem = geofences.get(0);
            generalEvent.setLocationId(geofenceItem.getId());
            generalEvent.setLocationName(geofenceItem.getName());
        }
    }

    /**
     * Sets the location ID and name on the given {@link GeneralLocation} using the first geofence in the list.
     * If the list is null or empty, no changes are made.
     *
     * @param geofences       The list of geofence items, from which the first item is used.
     * @param generalLocation The GeneralLocation to update with location information.
     */
    public static void setLocationNameAndId(List<GeofenceItem> geofences, GeneralLocation generalLocation) {
        if (geofences != null && !geofences.isEmpty()) {
            GeofenceItem geofenceItem = geofences.get(0);
            generalLocation.setLocationId(geofenceItem.getId());
            generalLocation.setLocationName(geofenceItem.getName());
        }
    }

    /**
     * Sets the location ID and name on the given {@link GeneralLocation} from the provided {@link GeofenceItem}.
     * If the geofence item is null, no changes are made.
     *
     * @param geofenceItem    The geofence item to extract location information from.
     * @param generalLocation The GeneralLocation to update with location information.
     */
    public static void setLocationNameAndId(GeofenceItem geofenceItem, GeneralLocation generalLocation) {
        if (geofenceItem != null) {
            generalLocation.setLocationName(geofenceItem.getName());
            generalLocation.setLocationId(geofenceItem.getId());
        }
    }

    /**
     * Sets the location ID and name on the given {@link GeneralEvent} from the provided {@link GeofenceItem}.
     * If the geofence item is null, no changes are made.
     *
     * @param geofenceItem  The geofence item to extract location information from.
     * @param generalEvent  The GeneralEvent to update with location information.
     */
    public static void setLocationNameAndId(GeofenceItem geofenceItem, GeneralEvent generalEvent) {
        if (geofenceItem != null) {
            generalEvent.setLocationName(geofenceItem.getName());
            generalEvent.setLocationId(geofenceItem.getId());
        }
    }

    /**
     * Configures checkpointing options for a Flink StreamExecutionEnvironment.
     *
     * @param env                       the StreamExecutionEnvironment to configure
     * @param checkPointInterval        the checkpoint interval in milliseconds
     * @param minTimeBetweenCheckpoints the minimum time between checkpoints in milliseconds
     */
    public static void setGenericDataStreamOptions(StreamExecutionEnvironment env, long checkPointInterval, long minTimeBetweenCheckpoints) {
        // Set RocksDB as the state backend
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
                // Enable checkpointing with specified interval
                .enableCheckpointing(checkPointInterval);

        // Set min time between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minTimeBetweenCheckpoints);

        // Set checkpoint timeout
        env.getCheckpointConfig().setCheckpointTimeout((int) (checkPointInterval * 20));

        // Limit to 1 concurrent checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // Clean up checkpoints when cancelled
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
    }

    /**
     * Parses a string in RFC 3339 format to a Unix timestamp.
     *
     * @param dt the RFC 3339 datetime string
     * @return the Unix timestamp in milliseconds, or -1 if parsing fails
     */
    public static long parseRfc3339DateTime(String dt) {
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(dt, DateTimeFormatter.ISO_OFFSET_DATE_TIME);

            // Convert to Unix timestamp
            return zdt.toInstant().toEpochMilli();
        } catch (Exception e) {
            // Return -1 if parsing fails
            return -1;
        }
    }

    /**
     * Retrieves the formatted event type based on the original event type and source.
     *
     * @param eventType the original event type
     * @param source    the source of the event
     * @return the formatted event type
     * @throws Exception if an error occurs while retrieving the mapping
     */
    public static String getFormattedEvent(String eventType, String source) throws Exception {
        EventMapping mapping = new EventMappingManager(source).getMapping();
        List<EventMapping.ValueMapping> events = mapping.getEvents();
        for (EventMapping.ValueMapping valuemap : events) {
            if (valuemap.getOriginal().equals(eventType)) {
                // Return the mapped event type
                return valuemap.getNew_type();
            }
        }
        return null;
    }

    /**
     * Determines the status of a vehicle based on its speed.
     *
     * @param speed The current speed of the vehicle. If null, the status will be "UNKNOWN".
     * @return A string representing the vehicle's status:
     * - "MOVING" if speed is greater than 5,
     * - "IDLE" if speed is between 1 and 5,
     * - "PARKED" if speed is less than or equal to 1,
     * - "UNKNOWN" if speed is null.
     */
    public static String getStatus(Double speed) {
        // Check if speed is null, if so return "UNKNOWN"
        if (speed == null) {
            return "UNKNOWN";
        }

        // Check if speed is greater than 5, return "MOVING"
        if (speed > 5) {
            return "MOVING";
        }

        // Check if speed is between 1 and 5 (exclusive), return "IDLE"
        if (speed <= 5 && speed > 1) {
            return "IDLE";
        }

        // If speed is less than or equal to 1, return "PARKED"
        return "PARKED";
    }

    /**
     * Parses a latitude string in the format "DDMM.MMMM" (degrees, minutes, and seconds) and converts it to a decimal representation.
     * The string must include a directional indicator at the end ('N' for North or 'S' for South).
     *
     * @param input The input string representing the latitude, which should be in the format "DDMM.MMMM[N/S]".
     * @return The latitude as a double in decimal format.
     * - Positive value for Northern Hemisphere (indicated by 'N'),
     * - Negative value for Southern Hemisphere (indicated by 'S').
     */
    public static double parseLatitude(String input) {
        // Extract the degrees (first two characters of the input)
        String degrees = input.substring(0, 2);

        // Extract the minutes (substring between degrees and the decimal point)
        String minutes = input.substring(2, input.indexOf("."));

        // Extract the seconds (substring after the decimal point, excluding the last character which is the hemisphere direction)
        String seconds = input.substring(input.indexOf("."), input.length() - 1);
        double value;
        try {
            // Parse the numeric parts (degrees, minutes, seconds) and convert to decimal format
            value = Double.parseDouble(degrees) + ((Double.parseDouble(minutes) + Double.parseDouble(seconds)) / 60);
        } catch (Exception e) {
//            e.printStackTrace();
            throw new IllegalArgumentException("Invalid latitude format: " + input, e);
//            return 0;
        }

        // Check if the latitude is in the Southern Hemisphere ('S') and adjust the sign accordingly
        if (input.endsWith("S")) {
            value *= -1; // If 'S' is found, the latitude is negative (Southern Hemisphere)
        }

        // Return the decimal representation of the latitude
        return value;
    }

    /**
     * Parses a longitude string in the format "DDDMM.MMMM" (degrees, minutes, and seconds) and converts it to a decimal representation.
     * The string must include a directional indicator at the end ('E' for East or 'W' for West).
     *
     * @param input The input string representing the longitude, which should be in the format "DDDMM.MMMM[E/W]".
     * @return The longitude as a double in decimal format.
     * - Positive value for Eastern Hemisphere (indicated by 'E'),
     * - Negative value for Western Hemisphere (indicated by 'W').
     */
    public static double parseLongitude(String input) {
        // Extract the degrees (first three characters of the input)
        String degrees = input.substring(0, 3);

        // Extract the minutes (substring between degrees and the decimal point)
        String minutes = input.substring(3, input.indexOf("."));

        // Extract the seconds (substring after the decimal point, excluding the last character which is the hemisphere direction)
        String seconds = input.substring(input.indexOf("."), input.length() - 1);

        double value;

        try {
            // Parse the numeric parts (degrees, minutes, seconds) and convert to decimal format
            value = Double.parseDouble(degrees) + ((Double.parseDouble(minutes) +  Double.parseDouble(seconds))/ 60);
        } catch (Exception e) {
//            e.printStackTrace();
            throw new IllegalArgumentException("Invalid longitude format: " + input, e);
//            return 0;
        }

        // Check if the longitude is in the Western Hemisphere ('W') and adjust the sign accordingly
        if (input.endsWith("W")) {
            value *= -1; // If 'W' is found, the longitude is negative (Western Hemisphere)
        }

        // Return the decimal representation of the longitude
        return value;
    }

    /**
     * Converts a cardinal or intercardinal direction (N, NE, E, SE, S, SW, W, NW) into its corresponding angle in degrees.
     * The returned value represents the direction in a 360-degree compass system.
     *
     * @param orientation The orientation string, which can be one of the following values: "N", "NE", "E", "SE", "S", "SW", "W", "NW".
     * @return The direction in degrees:
     * - "N" -&gt; 0.0
     * - "NE" -&gt; 45.0
     * - "E" -&gt; 90.0
     * - "SE" -&gt; 135.0
     * - "S" -&gt; 180.0
     * - "SW" -&gt; 225.0
     * - "W" -&gt; 270.0
     * - "NW" -&gt; 315.0
     * - Returns 999.0 if an unrecognized orientation is provided.
     */
    public static double calculateDirection(String orientation) {
        // Evaluate the input orientation and return the corresponding degree value
        switch (orientation) {
            case "N": {
                return 0.0;
            }
            case "NE": {
                return 45.00;
            }
            case "E": {
                return 90.00;
            }
            case "SE": {
                return 135.00;
            }
            case "S": {
                return 180.00;
            }
            case "SW": {
                return 225.00;
            }
            case "W": {
                return 270.00;
            }
            case "NW": {
                return 315.00;
            }
        }

        // Return a default value (999.0) if the orientation is not recognized
        return 999.00;
    }

    /**
     * Calculates the distance between two geographic points (latitude and longitude) using the Haversine formula.
     * The distance is returned in kilometers.
     *
     * @param point1 The first geographic point, represented by a LatLong object containing latitude and longitude.
     * @param point2 The second geographic point, represented by a LatLong object containing latitude and longitude.
     * @return The distance between the two points in kilometers.
     */
    public static double getDistanceBetweenPoints(LatLong point1, LatLong point2) {
        // Convert the latitude and longitude of the first point from degrees to radians
        double lat1R = Math.toRadians(point1.getLat());
        double lon1R = Math.toRadians(point1.getLng());

        // Convert the latitude and longitude of the second point from degrees to radians
        double lat2R = Math.toRadians(point2.getLat());
        double lon2R = Math.toRadians(point2.getLng());

        // Apply the Haversine formula to calculate the central angle between the two points
        double distance = Math.acos(Math.sin(lat1R) * Math.sin(lat2R) +
                Math.cos(lat1R) * Math.cos(lat2R) * Math.cos(lon2R - lon1R));

        // Multiply the result by the radius of the Earth (6371 km) to get the distance in kilometers
        distance *= 6371;

        // Return the calculated distance
        return distance;
    }

    /**
     * Retrieves metadata associated with a device using its serial number and configuration.
     * The method performs validation on the serial number and fetches device information from a cache.
     * It populates the TmMetadata object with relevant information such as asset ID, organization ID, and control rooms.
     *
     * @param rawSerial The raw serial number of the device.
     * @param config    The configuration object that contains necessary settings like the data cache server.
     * @param jobName   The name of the job requesting the metadata.
     * @return A TmMetadata object containing the device's metadata, or null if any validation fails.
     */
    public static TmMetadata getMetaData(String rawSerial, Config config, String jobName) {
        // Clean and validate the serial number
        String serial = cleanSerial(rawSerial);
        if (serial == null) { // If the serial number is null, return null
//            Logger.log(jobName+": null serial detected. serial="+rawSerial);
            return null;
        }

        // If the serial number is marked as UNKNOWN, return null
        if (serial.equals(UNKNOWN)) {
//            Logger.log(jobName+": unknown serial detected. serial="+rawSerial);
            return null;
        }

        // Get the server configuration from the provided config object
        String server = config.getDatacacheServer();

        // Retrieve the device information from the cache using the serial number
        TmDevices.TmDevice device = CacheUtils.getDeviceFromCache(server, serial);
        if (device == null) { // If no device is found in the cache, return null
//            Logger.log(jobName+": unknown device. serial="+rawSerial);
            return null;
        }

        // Validate if the device has both the orgId and deviceId; if either is missing or empty, return null
        if (device.getOrgId() == null || device.getDeviceId() == null || device.getOrgId().isEmpty() || device.getDeviceId().isEmpty()) {
//            Logger.log(jobName+": asset or org ID is null. orgId="+ device.getOrgId() +", assetId="+ device.getAssetId());
            return null;
        }

//         Initialize an empty TmMetadata object to store the metadata
        TmMetadata tmMetadata = new TmMetadata();

        // Check if the device has an active load, if so, extract stop IDs and set them in the metadata
        if (device.getTpsActiveLoad() == null) {
            tmMetadata.setStopIds(null); // No active load means no stop IDs
        } else {
            // Extract stop IDs from the active load and set them in the metadata
            List<String> stopIds = new ArrayList<>();
            for (Stop stop : device.getTpsActiveLoad().getStops()) {
                stopIds.add(stop.getLocationId());
            }
            tmMetadata.setStopIds(stopIds);
        }

        // Set additional metadata fields from the device information
        tmMetadata.setActiveLoad(device.getTpsActiveLoad());
        tmMetadata.setAssetId(device.getAssetId());
        tmMetadata.setOrgId(device.getOrgId());
        tmMetadata.setSiteIds(device.getSiteIds());

        // If available, set the routing violations from the organization's configuration
        if (device.getTpsOrgConfig() != null) {
            tmMetadata.setRoutingViolations(device.getTpsOrgConfig().getRoutingViolations());
        }

        // Set control rooms from the device information
        tmMetadata.setControlRooms(device.getControlRooms());

//        Logger.log("Collecting "+ jobName + " asset: "+tmMetadata.getAssetId() + " org: " + tmMetadata.getOrgId());

        // Return the populated TmMetadata object
        return tmMetadata;
    }

    private static ObjectMapper om = new ObjectMapper();

    /**
     * Sends an HTTP POST request to calculate distance and time based on the provided payload.
     * The request is sent to the mapping service URL, and the response is deserialized into
     * a DistanceAndTimeResp object.
     *
     * @param payload The DistanceAndTimeReqBody object containing request data.
     * @return A DistanceAndTimeResp object containing the distance and time data, or null if the request fails or an error occurs.
     */
    public static DistanceAndTimeResp getDistanceAndTime(DistanceAndTimeReqBody payload) {
        // Create a CloseableHttpClient to manage HTTP requests
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

            // Initialize the HTTP POST request with the target URL for distance calculation
            HttpPost req = new HttpPost(mappingURL + "/distance");

            // Serialize the payload into JSON format and set it as the request body
            String reqBodyString = om.writeValueAsString(payload);
            req.setEntity(new StringEntity(reqBodyString));

            // Set the headers to specify JSON content type for request and response
            req.setHeader("Accept", "application/json");
            req.setHeader("Content-type", "application/json");

            // Execute the HTTP POST request and capture the response
            try (CloseableHttpResponse resp = client.execute(req)) {

                // Check if the response status is 200 (OK); if not, return null
                if (resp.getStatusLine().getStatusCode() != 200) {
                    return null;
                }

                // Deserialize the response content into a DistanceAndTimeResp object and return it
                try (InputStream is = resp.getEntity().getContent()) {
                    return om.readValue(is, DistanceAndTimeResp.class);
                } finally {
                    EntityUtils.consumeQuietly(resp.getEntity());
                }
            }
        } catch (Exception e) {
            Logger.log("Error in getting the distance and Time: " + e.getMessage(), 5600);
            e.printStackTrace();

            // Return null if an error occurs
            return null;
        }
    }

    /**
     * Sends an HTTP POST request to retrieve the speed limit for a given geographical location.
     * The request is sent to the mapping service URL, and the response is deserialized into
     * a SpeedLimitResponse object.
     *
     * @param body The LatLong object containing latitude and longitude coordinates.
     * @return A SpeedLimitResponse object containing speed limit information, or null if the request fails or an error occurs.
     */
    public static SpeedLimitResponse getSpeedLimit(LatLong body) {
        // Create a CloseableHttpClient to manage HTTP requests
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

            // Initialize the HTTP POST request with the target URL for speed limit information
            HttpPost req = new HttpPost(mappingURL + "/speedLimit");


            // Serialize the LatLong object into JSON format and set it as the request body
            String reqBodyString = om.writeValueAsString(body);
            req.setEntity(new StringEntity(reqBodyString));

            // Set headers to specify JSON content type for both request and response
            req.setHeader("Accept", "application/json");
            req.setHeader("Content-type", "application/json");

            // Execute the HTTP POST request and capture the response
            try (CloseableHttpResponse resp = client.execute(req)) {

                // Check if the response status is 200 (OK); if not, return null
                if (resp.getStatusLine().getStatusCode() != 200) {
                    return null;
                }

                // Deserialize the response content into a SpeedLimitResponse object and return it
                try (InputStream is = resp.getEntity().getContent()) {
                    return om.readValue(is, SpeedLimitResponse.class);
                } finally {
                    EntityUtils.consumeQuietly(resp.getEntity());
                }
            }
        } catch (Exception e) {
            Logger.log("Error in getting the max speed limit: " + e.getMessage(), 5600);
            e.printStackTrace();

            // Return null if an error occurs
            return null;
        }
    }

    /**
     * Retrieves a list of valid control room IDs based on the provided event type, source, and list of control rooms.
     * Filters through the control rooms to find matches where the classification type and source meet specified criteria.
     *
     * @param event        The event type, which will be prefixed to "TPS_" to match classification types.
     * @param source       The source to match against control room sources.
     * @param controlRooms A list of ControlRoom objects to check.
     * @return A list of control room IDs that meet the specified event and source criteria, or an empty list if no matches are found.
     */
    public static List<String> getControlRooms(String event, String source, List<ControlRoom> controlRooms) {
        // Initialize a list to store IDs of valid control rooms
        List<String> validControlRooms = new ArrayList<>();

        // If the list of control rooms is null, return the empty validControlRooms list
        if (controlRooms == null) {
            return validControlRooms;
        }

        // Iterate through each control room in the provided list
        for (ControlRoom controlRoom : controlRooms) {

            // Skip processing if this control room's ID is already added to the validControlRooms list
            if (validControlRooms.contains(controlRoom.getId())) {
                continue;
            }

            // Loop through each ClassificationTypeSources associated with the current control room
            for (ClassificationTypeSources classificationTypeSources : controlRoom.getClassificationTypeSources()) {
                // Flag to track if the current control room has been added
                boolean added = false;
                // Check if the classification type matches "TPS_" + event
                if (classificationTypeSources.getClassificationType().equals("TPS_" + event)) {
                    // Check if the source exists in the TPS sources of this classification type
                    for (String s : classificationTypeSources.getTpsSources()) {
                        if (s.equals(source)) {
                            // Add control room ID to the list
                            validControlRooms.add(controlRoom.getId());

                            // Mark as added to skip further checks for this control room
                            added = true;
                            break;
                        }
                    }
                }
                // Break from the loop if the control room has already been added
                if (added) {
                    break;
                }
            }
        }

        // Return the list of valid control room IDs
        return validControlRooms;
    }

    /**
     * Processes geofence events based on the current mapping of geofences, current geofence items,
     * speed, and the active status of the device. It creates new geofence events for arrival or departure
     * depending on the speed and presence of geofences.
     *
     * @param mapping          A map where keys represent geofence IDs and values represent geofence names.
     * @param currentGeofences A list of currently active geofence items around the device.
     * @param speed            The current speed of the device, used to determine arrival or departure events.
     * @param on               Indicates if geofencing is active.
     * @return A Pair containing a list of GeofenceToBeCreated events and the updated mapping.
     */
    public static Pair<List<GeofenceToBeCreated>, Map<String, String>> geofenceEvent(Map<String, String> mapping, List<GeofenceItem> currentGeofences, double speed, boolean on) {
        // List to store new geofence events to be created
        List<GeofenceToBeCreated> geofenceEvents = new ArrayList<>();

        // If geofencing is turned off, return the empty event list and the original mapping
        if (!on) {
            return new Pair<>(geofenceEvents, mapping);
        }

        // Case 1: No current geofences, non-empty mapping, and speed >= 5 indicates departure events
        if (currentGeofences == null && !mapping.isEmpty() && speed >= 5) {
            Iterator<Map.Entry<String, String>> iterator = mapping.entrySet().iterator();
            while (iterator.hasNext()) {
                GeofenceToBeCreated geofenceEvent = new GeofenceToBeCreated();
                Map.Entry<String, String> entry = iterator.next();
                geofenceEvent.setId(entry.getKey());
                geofenceEvent.setName(entry.getValue());
                geofenceEvent.setType(GeofenceEvent.SUBTYPE_DEPARTURE);
                geofenceEvents.add(geofenceEvent);
                iterator.remove();// Remove entry from mapping after processing
            }
        }
        // Case 2: Active geofences present, empty mapping, and speed < 5 indicates arrival events
        else if (currentGeofences != null && mapping.isEmpty() && speed < 5) {
            for (GeofenceItem geofenceItem : currentGeofences) {
                if (mapping.containsKey(geofenceItem.getId())) {
                    continue;
                }
                GeofenceToBeCreated geofenceEvent = new GeofenceToBeCreated();
                geofenceEvent.setType(GeofenceEvent.SUBTYPE_ARRIVAL);
                geofenceEvent.setId(geofenceItem.getId());
                geofenceEvent.setName(geofenceItem.getName());
                geofenceEvents.add(geofenceEvent);
                mapping.put(geofenceItem.getId(), geofenceItem.getName());// Update mapping with new arrivals
            }
        }
        // Case 3: Active geofences and a non-empty mapping, handle arrival or departure based on speed
        else if (currentGeofences != null && !mapping.isEmpty()) {
            // Handle arrivals if speed < 5
            if (speed < 5) {
                for (GeofenceItem geofenceItem : currentGeofences) {
                    if (!mapping.containsKey(geofenceItem.getId())) {
                        GeofenceToBeCreated geofenceEvent = new GeofenceToBeCreated();
                        geofenceEvent.setType(GeofenceEvent.SUBTYPE_ARRIVAL);
                        geofenceEvent.setId(geofenceItem.getId());
                        geofenceEvent.setName(geofenceItem.getName());
                        geofenceEvents.add(geofenceEvent);
                        mapping.put(geofenceItem.getId(), geofenceItem.getName()); // Add new arrivals to mapping
                    }
                }
            }

            // Handle departures for geofences in the mapping but not in currentGeofences if speed >= 5
            Iterator<Map.Entry<String, String>> iterator = mapping.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                boolean contains = currentGeofences.stream().anyMatch(it -> Objects.equals(entry.getKey(), it.getId()));
                if (!contains && speed >= 5) {
                    GeofenceToBeCreated geofenceEvent = new GeofenceToBeCreated();
                    geofenceEvent.setId(entry.getKey());
                    geofenceEvent.setName(entry.getValue());
                    geofenceEvent.setType(GeofenceEvent.SUBTYPE_DEPARTURE);
                    geofenceEvents.add(geofenceEvent);
                    iterator.remove(); // Remove departed geofence from mapping
                }
            }
        }

        // Return the list of generated geofence events and the updated mapping
        return new Pair<>(geofenceEvents, mapping);
    }

    /**
     * Processes geofence events based on the current mapping of geofences, current geofence items,
     * speed, and the active status of the device. It creates new geofence events for arrival or departure
     * depending on the speed and presence of geofences.
     *
     * @param mapping                       A map where keys represent geofence IDs and values represent geofence names.
     * @param currentGeofences              A list of currently active geofence items around the device.
     * @param speed                         The current speed of the device, used to determine arrival or departure events.
     * @param on                            Indicates if geofencing is active.
     * @param currentArrivedAtGeofences     The ids of the geofences the asset is currently in.
     * @return A Pair containing a list of GeofenceToBeCreated events and the updated mapping.
     */
    public static Pair<List<GeofenceToBeCreated>, Map<String, String>> geofenceEvent(Map<String, String> mapping, List<GeofenceItem> currentGeofences, double speed, boolean on, Set<String> currentArrivedAtGeofences) {
        // List to store new geofence events to be created
        List<GeofenceToBeCreated> geofenceEvents = new ArrayList<>();

        // If geofencing is turned off, return the empty event list and the original mapping
        if (!on) return new Pair<>(geofenceEvents, mapping);

        Set<String> currentGeofenceIds = new HashSet<>();
        if (currentGeofences != null) {
            for (GeofenceItem item : currentGeofences) {
                String id = item.getId();
                currentGeofenceIds.add(id);
                if (speed < 5 && !currentArrivedAtGeofences.contains(id)) {
                    // ARRIVAL condition met
                    GeofenceToBeCreated arrival = new GeofenceToBeCreated();
                    arrival.setId(id);
                    arrival.setName(item.getName());
                    arrival.setType(GeofenceEvent.SUBTYPE_ARRIVAL);
                    geofenceEvents.add(arrival);

                    mapping.put(item.getId(), item.getName());
                    currentArrivedAtGeofences.add(id);
                }
            }
        }

        // Emit departures for geofences we are no longer in
        if (speed >= 5 && !currentArrivedAtGeofences.isEmpty()) {
            Set<String> departed = new HashSet<>();

            for (String arrivedId : currentArrivedAtGeofences) {
                if (!currentGeofenceIds.contains(arrivedId) && mapping.containsKey(arrivedId)) {
                    GeofenceToBeCreated departure = new GeofenceToBeCreated();
                    departure.setId(arrivedId);
                    departure.setName(mapping.get(arrivedId));
                    departure.setType(GeofenceEvent.SUBTYPE_DEPARTURE);
                    geofenceEvents.add(departure);

                    mapping.remove(arrivedId);
                    departed.add(arrivedId);
                }
            }

            currentArrivedAtGeofences.removeAll(departed);
        }

        return new Pair<>(geofenceEvents, mapping);
    }

    /**
     * Converts a date string into a Unix timestamp (in seconds) based on a specified date format.
     *
     * @param date   The date string to be converted.
     * @param format The format of the date string, following SimpleDateFormat patterns.
     * @return The Unix timestamp in seconds.
     * @throws ParseException If the date string does not match the specified format.
     */
    public static long getTimeFromString(String date, String format) throws ParseException {
        // Initialize a SimpleDateFormat object with the provided format
        SimpleDateFormat dateParser = new SimpleDateFormat(format);

        // Parse the date string and convert it to a Unix timestamp in seconds
        return dateParser.parse(date).getTime() / 1000;
    }

    /**
     * Checks if the current location is an unplanned stop by verifying geofence IDs.
     *
     * @param metadata         The metadata object containing stop IDs.
     * @param currentGeofences A map of current geofences with their IDs.
     * @return {@code true} if the location is an unplanned stop (no match between stop IDs and geofence IDs), otherwise {@code false}.
     */
    public static boolean checkUnplannedStop(TmMetadata metadata, Map<String, String> currentGeofences) {
        // If there are no planned stop IDs in the metadata, it's not an unplanned stop
        if (metadata.getStopIds() == null) {
            return false;
        }

        // Loop through each geofence ID in the current geofences
        for (String geofenceId : currentGeofences.keySet()) {
            // If a geofence ID matches any of the stop IDs in metadata, it's not an unplanned stop
            if (metadata.getStopIds().contains(geofenceId)) {
                return false;
            }
        }

        // If no matching geofence ID is found in stop IDs, the location is an unplanned stop
        return true;
    }

    /**
     * Creates a temperature event with temperature data for up to three setpoints, discharge, and return air readings.
     *
     * @param parameters Contains all the parameters required to create the temperature event.
     * @return The created {@link TemperatureEvent}, or {@code null} if no valid temperature data is provided.
     */
    public static TemperatureEvent createTemperatureEvent(TempEventParameters parameters) {

        TmMetadata metadata = parameters.getMetadata();
        Double setpoint1 = parameters.getSetPoint1();
        Double setpoint2 = parameters.getSetPoint2();
        Double setpoint3 = parameters.getSetPoint3();
        Double dischargeAir1 = parameters.getDischargeAir1();
        Double dischargeAir2 = parameters.getDischargeAir2();
        Double dischargeAir3 = parameters.getDischargeAir3();
        Double returnAir1 = parameters.getReturnAir1();
        Double returnAir2 = parameters.getReturnAir2();
        Double returnAir3 = parameters.getReturnAir3();
        double lat = parameters.getLatitude();
        double lon = parameters.getLongitude();
        String source = parameters.getSource();

        // Initialize a new TemperatureEvent with basic information
        TemperatureEvent event = new TemperatureEvent(TemperatureEvent.TEMPERATURE_TYPE, lat, lon, parameters.getCreated(), parameters.getSerial(), metadata, source);


        // Add the first set of temperature data if all values are non-null
        if (setpoint1 != null || dischargeAir1 != null || returnAir1 != null) {
            event.getTemperatureData().add(new TemperatureEvent.TemperatureEventData(TemperatureEvent.TEMPERATURE1_SUBTYPE, setpoint1, dischargeAir1, returnAir1));
        }

        // Add the second set of temperature data if all values are non-null
        if (setpoint2 != null || dischargeAir2 != null || returnAir2 != null) {
            event.getTemperatureData().add(new TemperatureEvent.TemperatureEventData(TemperatureEvent.TEMPERATURE2_SUBTYPE, setpoint2, dischargeAir2, returnAir2));
        }

        // Add the third set of temperature data if all values are non-null
        if (setpoint3 != null || dischargeAir3 != null || returnAir3 != null) {
            event.getTemperatureData().add(new TemperatureEvent.TemperatureEventData(TemperatureEvent.TEMPERATURE3_SUBTYPE, setpoint3, dischargeAir3, returnAir3));
        }

        // If no temperature data was added, return null to indicate an invalid event
        if (event.getTemperatureData().isEmpty()) {
            return null;
        }

        if (parameters.isTempEvent()) {
            event.setSource(parameters.getSource());
            String server = parameters.getServer();
            List<GeofenceItem> geofences = CacheUtils.isPointInGeofences(server, metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

            if (geofences != null && !geofences.isEmpty()) {
                event.setLocationId(geofences.get(0).getId());
            }
        }

        // Return the constructed event with populated temperature data
        return event;
    }

    /**
     * Converts a date string from the original format another format.
     * <p>
     * The method attempts to parse the given date string according to the predefined
     * `dateFormatOriginal`. If parsing fails, the current UTC date and time are used as a fallback.
     * </p>
     *
     * @param dateString            The date string to be converted, expected in `dateFormatOriginal`.
     * @param dateFormatOriginal    The format of the dateString.
     * @param dateFormatNew         The format that we want the dateString to be in.
     * @return The formatted date string in `dateFormatNew`.
     */
    public static String convertDate(String dateString, String dateFormatOriginal, String dateFormatNew) {
        // Formatter to parse the input date string using the original date format.
        DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(dateFormatOriginal);
        LocalDateTime localDateTime;

        try {
            // Attempt to parse the date string using the original format.
            localDateTime = LocalDateTime.parse(dateString, dtFormatter);
        } catch (Exception e) {
            // Log an error message if parsing fails and set to the current date/time.
            Logger.log("Error in Parsing localDateTime, the date string is: " + dateString + "\nError Message" + e.getMessage(), 5600);
            localDateTime = LocalDateTime.now();
        }

        // Convert the LocalDateTime to OffsetDateTime in UTC timezone.
        OffsetDateTime offsetDateTime = localDateTime.atOffset(ZoneOffset.UTC);
        // Formatter for the output date string in the converted date format.
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern(dateFormatNew);

        // Return the date formatted in the converted format.
        return offsetDateTime.format(outputFormatter);
    }

    /**
     * Converts a given {@link Date} to a UTC timestamp in seconds based on a timezone offset.
     *
     * @param date The date to convert.
     * @param zone The timezone offset string (e.g., "+2").
     * @return The UTC timestamp in seconds adjusted for the timezone, or 0 if zone is not recognized.
     */
    public static long getUTCTimeFromZone(Date date, String zone) {
        switch (zone) {
            case "+2":
                return (date.getTime() / 1000) - 7200;
        }
        return 0;
    }

    /**
     * Normalizes a date string by ensuring that timezone offsets have minutes included.
     * <p>
     * If the date string ends with a timezone offset in the form ±HH, ":00" is appended.
     * For example, "2025-07-24T12:00:00+02" becomes "2025-07-24T12:00:00+02:00".
     *
     * @param dateString The date string to normalize.
     * @return The normalized date string.
     */
    public static String normalizeDateOffset(String dateString) {
        if (dateString.matches(".*[+-]\\d{2}$")) {
            return dateString + ":00";
        }
        return dateString;
    }

    /**
     * Returns the current system timestamp in seconds since the Unix epoch.
     *
     * @return The current timestamp in seconds.
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Creates a deep copy of a serializable object using serialization.
     *
     * @param <T>    The type of the object.
     * @param object The object to copy.
     * @return A deep copy of the object.
     * @throws RuntimeException if serialization or deserialization fails.
     */
    public static <T extends Serializable> T deepCopy(T object) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                @SuppressWarnings("unchecked")
                T copy = (T) ois.readObject();
                return copy;
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error during deep copying", e);
        }
    }

    /**
     * Checks if a given point lies within or on the boundary of a circle defined by a center point and radius.
     *
     * @param point  The point to check.
     * @param centre The center of the circle.
     * @param radius The radius of the circle.
     * @return True if the point is inside or on the boundary of the circle; false otherwise.
     */
    public boolean isPointInCircle(LatLong point, LatLong centre, double radius) {
        double distance = getDistanceBetweenPoints(point, centre);
        return distance <= radius;
    }

    /**
     * Checks if a given point lies within a bounding box defined by minimum and maximum latitude/longitude.
     *
     * @param point       The point to check.
     * @param boundingBox The bounding box with min and max latitude/longitude.
     * @return True if the point lies within the bounding box; false otherwise.
     */
    public boolean isPointInPolygon(LatLong point, Geofences.MinMaxLatLong boundingBox) {
        return point.getLat() >= boundingBox.getMin().getLat() &&
                point.getLat() <= boundingBox.getMax().getLat() &&
                point.getLng() >= boundingBox.getMin().getLng() &&
                point.getLng() <= boundingBox.getMax().getLng();
    }
}
