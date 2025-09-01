package za.co.trackmatic.flink.flatmaps.paragon;

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
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitRequest;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitResponse;
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.trips.*;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Flink RichFlatMapFunction that processes raw Paragon trip data ({@link ParagonRawData})
 * and manages trip and event message states for each device.
 * <p>
 * This processor handles GPS and metadata information from vehicles to track trip start,
 * continuation, and end events. It maintains stateful trip and event messages per vehicle,
 * determines trip status (e.g., "IDLE", "MOVING", "SPEEDING") based on speed and speed limits,
 * and emits trip summaries downstream.
 * </p>
 * <p>
 * Trip data is stored and updated using Flink's managed keyed state to allow fault-tolerant,
 * scalable stream processing.
 * </p>
 */
public class ParagonTripProcessorFlatmap extends RichFlatMapFunction<ParagonRawData, TripTopicResponse> implements Serializable {

    private Config config;

    private final String dateFormatOriginal = "yyyy-MM-dd HH:mm:ss";
    private final String dateFormatConverted = "yyyy-MM-dd'T'HH:mm:ss'Z'";


    public ParagonTripProcessorFlatmap(Config config) {
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
                .cleanupIncrementally(10000,true)
                .build();

        // Define a MapState descriptor to hold mapping between serial numbers and trip data messages.
        // This map state will store the TripGeoMessage and TripEventMessage tuples for each serial (device ID).
        MapStateDescriptor<String, Tuple2<TripGeoMessage, TripEventMessage>> descriptor = new MapStateDescriptor<>("paragonTripMap",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<TripGeoMessage, TripEventMessage>>() {}));
        descriptor.enableTimeToLive(stateTtlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    /**
     * Extracts initials from the first names and the full surname from a given full name.
     * <p>
     * The method returns a list where the first element is a concatenation of the initials
     * of all given names except the last one, and the second element is the surname (last name).
     * If the full name is null or empty, both elements in the list will be empty strings.
     * If there is only one name, the first letter of that name is returned as the initial,
     * and the surname is set to an empty string.
     * </p>
     *
     * @param fullName The full name as a single string with each name separated by spaces.
     * @return A list of two strings: the concatenated initials and the surname.
     */
    public List<String> getInitialsAndSurname(String fullName) {
        List<String> initialsAndSurname = new ArrayList<>();

        // Check if the input is null or empty, and return empty strings if so.
        if(fullName == null || fullName.isEmpty()){
            initialsAndSurname.add("");
            initialsAndSurname.add("");
            return initialsAndSurname;
        }

        // Split the full name by spaces, limiting to a maximum of 10 names (arbitrary upper limit).
        String[] names = fullName.split(" ", 10);


        // Handle cases based on the number of names found.
        if (names.length == 0) {
            initialsAndSurname.add("");
            initialsAndSurname.add("");

        } else if (names.length == 1) {
            // If there is only one name, add its initial and leave the surname empty.
            initialsAndSurname.add(String.valueOf(names[0].charAt(0)));
            initialsAndSurname.add("");
        } else {
            // If there are multiple names, collect initials from all names except the last one.
            StringBuilder initials = new StringBuilder();
            for (int i = 0; i < names.length - 1; i++) {
                // Skip any empty name parts, which might result from extra spaces.
                if(names[i].isEmpty()){
                    continue;
                }
                initials.append(names[i].charAt(0));
            }
            initialsAndSurname.add(String.valueOf(initials));
            initialsAndSurname.add(String.valueOf(names[names.length - 1]));
        }

        // Return the list containing the initials and surname.
        return initialsAndSurname;
    }

    /**
     * Converts a date string from the original format to the specified converted format.
     * <p>
     * The method attempts to parse the given date string according to the predefined
     * `dateFormatOriginal`. If parsing fails, the current UTC date and time are used as a fallback.
     * </p>
     *
     * @param dateString The date string to be converted, expected in `dateFormatOriginal`.
     * @return The formatted date string in `dateFormatConverted`.
     */
    public String convertDate(String dateString){
        // Formatter to parse the input date string using the original date format.
        DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(dateFormatOriginal);
        LocalDateTime localDateTime;

        try {
            // Attempt to parse the date string using the original format.
            localDateTime = LocalDateTime.parse(dateString, dtFormatter);
        }catch (Exception e){
            // Log an error message if parsing fails and set to the current date/time.
            Logger.log("Error in Parsing localDateTime, the date string is: " + dateString + "\nError Message" + e.getMessage(), 5556);
            localDateTime = LocalDateTime.now();
        }

        // Convert the LocalDateTime to OffsetDateTime in UTC timezone.
        OffsetDateTime offsetDateTime = localDateTime.atOffset(ZoneOffset.UTC);
        // Formatter for the output date string in the converted date format.
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern(dateFormatConverted);

        // Return the date formatted in the converted format.
        return offsetDateTime.format(outputFormatter);
    }

    /**
     * Retrieves location information based on the provided organization ID, site IDs, latitude, and longitude.
     * <p>
     * The method checks if the provided latitude and longitude are inside any geofence defined for the given
     * organization and site IDs. If the point is within a geofence, the first geofence's information is returned.
     * If the point is not inside any geofence, null is returned.
     * </p>
     *
     * @param orgId The organization ID for the geofence query.
     * @param siteIds A list of site IDs for the geofence query.
     * @param lat The latitude of the point to check.
     * @param lon The longitude of the point to check.
     * @return A LocationInfo object containing the ID and name of the first geofence found, or null if no geofence contains the point.
     */
    public LocationInfo getLocation(String orgId, List<String> siteIds, double lat, double lon) {

        // Check if the given point (lat, lon) is inside any geofences for the given organization and site IDs.
        List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(config.getDatacacheServer(), orgId, new LatLong(lat, lon), siteIds);

        // If no geofences are found or the list is empty, return null.
        if (geofenceItems == null || geofenceItems.isEmpty()) {
            return null;
        }

        // Create a LocationInfo object to store the details of the first geofence.
        LocationInfo locationInfo = new LocationInfo();
        locationInfo.setId(geofenceItems.get(0).getId());
        locationInfo.setName(geofenceItems.get(0).getName());

        // Return the LocationInfo object with the geofence information.
        return locationInfo;
    }

    /**
     * Sets the start and end event location information for a given data item.
     * <p>
     * This method creates an EventLocation object and populates it with the location information,
     * date-time, and coordinates (latitude and longitude) extracted from the provided raw data,
     * along with the organization and site IDs to determine the relevant location.
     * </p>
     *
     * @param rawData The data item containing the GPS information to set the event location.
     * @param orgId The organization ID used to retrieve geofence information.
     * @param siteIds A list of site IDs for geofence lookup.
     * @param lat The latitude of the location to set.
     * @param lon The longitude of the location to set.
     * @return The EventLocation object populated with location details, date-time, and coordinates.
     */
    public EventLocation setStartAndEndEvent(DataItem rawData, String orgId, List<String> siteIds, double lat, double lon) {
        // Create a new EventLocation object to store event data.
        EventLocation eventLocation = new EventLocation();

        // Retrieve the location information based on the organization ID, site IDs, and coordinates.
        LocationInfo locationInfo = getLocation(orgId, siteIds, lat, lon);

        // Set the location information to the event location object.
        eventLocation.setLocation(locationInfo);

        // Convert and set the date-time for the event using the raw data's GPS timestamp.
        eventLocation.setDateTime(convertDate(rawData.getGps().getDt()));

        // Set the latitude and longitude of the event.
        eventLocation.setLat(lat);
        eventLocation.setLng(lon);

        // Return the populated EventLocation object.
        return eventLocation;
    }

    /**
     * Creates a TripGeoSnapshot from the provided data item and location details.
     * <p>
     * This method generates a snapshot of the trip's geographical information, including
     * the accuracy, direction, coordinates, location information, speed, and status.
     * Additional optional details such as satellite information and battery level (if available)
     * are also captured and included in the snapshot.
     * </p>
     *
     * @param dataItem The raw data item containing GPS and other sensor data to generate the snapshot.
     * @param lat The latitude of the geographical location to set in the snapshot.
     * @param lon The longitude of the geographical location to set in the snapshot.
     * @param orgId The organization ID used to retrieve geofence information.
     * @param siteIds A list of site IDs for geofence lookup.
     * @param speed The speed at the time of the snapshot.
     * @param status The current status of the trip (e.g., "MOVING", "IDLE").
     * @return A TripGeoSnapshot object populated with the geographical and sensor data.
     */
    public TripGeoSnapshot createGeoSnapshot(DataItem dataItem, double lat, double lon, String orgId, List<String> siteIds, double speed, String status) {

        // Create a new TripGeoSnapshot object to store the geographical and sensor data.
        TripGeoSnapshot tripGeoSnapshot = new TripGeoSnapshot();

        tripGeoSnapshot.setAccuracy(dataItem.getGps().getHd());
        tripGeoSnapshot.setDateTime(convertDate(dataItem.getGps().getDt()));
        tripGeoSnapshot.setDirection(dataItem.getGps().getCo());
        tripGeoSnapshot.setLat(lat);
        tripGeoSnapshot.setLng(lon);
        tripGeoSnapshot.setLocation(getLocation(orgId, siteIds, lat, lon));
        tripGeoSnapshot.getOptional().setSat(dataItem.getGps().getNs());
        tripGeoSnapshot.setSpeed(speed);
        tripGeoSnapshot.setStatus(status);

        // If available, set the battery level based on the ADC value from the raw data.
        if(dataItem.getAdc() != null){
            tripGeoSnapshot.setBatteryLevel((int)(100*dataItem.getAdc().getA1()/30));
        }

        // Return the populated TripGeoSnapshot object.
        return tripGeoSnapshot;
    }

    /**
     * Sets the initial trip message by populating the `tripGeoMessage` and `tripEventMessage`
     * with relevant context and driver details based on the provided raw data and data item.
     * <p>
     * This method sets the asset ID, load ID, device ID, site ID, organization ID, and trip ID
     * into the context of the trip messages. It also extracts and sets the driver's initials and
     * last name based on the provided driver name in the raw data.
     * </p>
     *
     * @param tripGeoMessage The `TripGeoMessage` object to be populated with context and driver details.
     * @param tripEventMessage The `TripEventMessage` object to be populated with context and driver details.
     * @param rawData The raw data containing metadata and other information about the trip.
     * @param dataItem The data item containing trip-specific information used to generate the trip ID.
     */
    public void setInitialTripMessage(TripGeoMessage tripGeoMessage, TripEventMessage tripEventMessage, ParagonRawData rawData, DataItem dataItem){
        Context c = new Context();
        if(rawData.getMetadata().getActiveLoad() != null){
            c.setLoadId(rawData.getMetadata().getActiveLoad().getLoadId());
            c.setSiteId(rawData.getMetadata().getActiveLoad().getSiteId());

            // Extract the initials and surname of the driver from the raw data.
            List<String> initAndSurname = getInitialsAndSurname(rawData.getMetadata().getActiveLoad().getDriverName());

            // Set the driver's initials and last name in the tripGeoMessage.
            tripGeoMessage.getDriverDetails().setInitials(initAndSurname.get(0));
            tripGeoMessage.getDriverDetails().setLastName(initAndSurname.get(1));
        }
        c.setAssetId(rawData.getMetadata().getAssetId());
        c.setDeviceId(Utils.cleanSerial(rawData.getSerial()));
        c.setOrgId(rawData.getMetadata().getOrgId());

        // Generate a unique trip ID by combining a UUID with the trip ID from the data item.
        String tripId = UUID.randomUUID() + "_"+ dataItem.getTid();
        c.setTripId(tripId);

        // Set the context for both the tripGeoMessage and tripEventMessage.
        tripGeoMessage.setContext(c);
        tripEventMessage.setContext(c);
    }

    /**
     * Checks if critical low conditions are met based on the provided data item.
     * This method updates the trip's computed values to reflect whether a critical low condition
     * has been avoided. If the ADC value A1 from the data item is below a specified threshold and
     * the trip's critical low condition has not yet been avoided, it marks the condition as not avoided.
     *
     * @param tripGeoMessage The TripGeoMessage object containing the trip details.
     * @param dataItem The DataItem object containing the ADC values to be checked.
     */
    public void checkCritical(TripGeoMessage tripGeoMessage, DataItem dataItem){
        // Checks if the ADC value A1 is less than 11.5 and if the critical low condition is currently avoided.
        if (dataItem.getAdc() != null && dataItem.getAdc().getA1() < 11.5 && tripGeoMessage.getComputedValues().isCriticalLowAvoided()) {
            // Marks the critical low condition as not avoided.
            tripGeoMessage.getComputedValues().setCriticalLowAvoided(false);
        }
    }

    /**
     * Sets the previous location and time details for a given trip.
     * This method updates the last known latitude, longitude, and timestamp for the trip,
     * which can be used to calculate distance and time intervals in subsequent updates.
     *
     * @param tripGeoMessage The TripGeoMessage object containing the trip details.
     * @param lat The latitude of the previous location.
     * @param lon The longitude of the previous location.
     * @param time The timestamp of the previous location in milliseconds.
     */
    public void setPreviousDetails(TripGeoMessage tripGeoMessage, double lat, double lon, long time){

        // Sets the last known latitude of the trip.
        tripGeoMessage.setLastLat(lat);

        // Sets the last known longitude of the trip.
        tripGeoMessage.setLastLon(lon);

        // Sets the last recorded timestamp for the trip.
        tripGeoMessage.setLastTime(time);
    }

    /**
     * Updates the computed values of a trip based on the provided parameters, including
     * speed, time, status, and location. It adjusts the total speed, steps, average speed,
     * maximum speed, driving and idle times, total time, and distance.
     *
     * @param tripGeoMessage  the message containing trip details and computed values.
     * @param speed           the current speed at this point in the trip.
     * @param time            the timestamp for the current update.
     * @param status          the status of the trip (e.g., "MOVING" or "IDLE").
     * @param lat             the latitude of the current position.
     * @param lon             the longitude of the current position.
     * @param incrementSteps  a boolean indicating whether to increment the step count.
     */
    public void updateComputedValues(TripGeoMessage tripGeoMessage,double speed, long time, String status, double lat, double lon, boolean incrementSteps){
        // Increases the total speed sum by the current speed.
        tripGeoMessage.increaseTotalSpeed(speed);

        // Increments the step count if incrementSteps is true.
        if(incrementSteps) {
            tripGeoMessage.incrementSteps();
        }

        // Calculates and updates the average speed based on total speed and steps.
        tripGeoMessage.getComputedValues().setAverageSpeed(tripGeoMessage.getTotalSpeed()/tripGeoMessage.getSteps());

        // Updates the maximum speed if the current speed exceeds the previously recorded maximum speed.
        if(speed > tripGeoMessage.getComputedValues().getMaxSpeed()){
            tripGeoMessage.getComputedValues().setMaxSpeed(speed);
        }

        // Calculates time difference and updates driving or idle time based on status.
        if(tripGeoMessage.getLastTime() != null){
            long timeDifference = time-tripGeoMessage.getLastTime();
            if (status.equals("MOVING") || status.equals("SPEEDING")) {
                tripGeoMessage.getComputedValues().incrementDrivingTime(timeDifference);
            }else {
                tripGeoMessage.getComputedValues().incrementIdleTime(timeDifference);
            }
        }

        // Updates the total time by summing driving and idle times.
        tripGeoMessage.getComputedValues().setTotalTime(tripGeoMessage.getComputedValues().getDrivingTime() + tripGeoMessage.getComputedValues().getIdleTime());

        // Calculates and increments the total distance using the last known position, if available.
        if(tripGeoMessage.getLastLat() != null && tripGeoMessage.getLastLon() != null){
            tripGeoMessage.getComputedValues().incrementTotalDistance(Utils.getDistanceBetweenPoints(new LatLong(tripGeoMessage.getLastLat(),tripGeoMessage.getLastLon()),new LatLong(lat,lon)) * 1000);
        }

    }

    /**
     * Converts a formatted time string into seconds by parsing it with the specified format
     * and dividing the result by a given factor.
     *
     * @param time         The time as a String to be parsed.
     * @param divideFactor The factor by which to divide the parsed time in milliseconds
     *                     to convert it into seconds (e.g., 1000 to convert milliseconds to seconds).
     * @param format       The format of the input time String (e.g., "HH:mm:ss").
     * @return             The time in seconds, calculated by parsing the time and dividing by the factor.
     * @throws ParseException If the time string cannot be parsed with the provided format.
     */
    public long returnTimeInSeconds(String time, int divideFactor, String format) throws ParseException {
        SimpleDateFormat dateParser = new SimpleDateFormat(format);
        return dateParser.parse(time).getTime()/divideFactor;
    }

    /**
     * Sets the event data for a trip, specifically handling speeding events based on the current status,
     * speed limit response, and location. This method creates or updates event snapshots depending on
     * the current and last recorded statuses and adds snapshots to the trip event message.
     *
     * @param dataItem           the current data item containing GPS information and speed.
     * @param tripEventMessage   the message containing trip events and last status.
     * @param speedLimitResponse the response containing the speed limit data for the current location.
     * @param status             the current trip status (e.g., "SPEEDING" or "MOVING").
     * @param lat                the latitude of the current location.
     * @param lon                the longitude of the current location.
     * @param server             the server used to check geofence membership.
     * @param metadata           metadata for the trip, including organization and site details.
     * @param tripGeoMessage     the message containing geographical data and computed values for the trip.
     * @throws ParseException    if there is an error parsing date or time values.
     */
    public void setEventData(DataItem dataItem, TripEventMessage tripEventMessage, SpeedLimitResponse speedLimitResponse, String status, double lat, double lon, String server, TmMetadata metadata, TripGeoMessage tripGeoMessage) throws ParseException {
        // If no speed limit response is available and the last status was not speeding, exit the method.
        if(speedLimitResponse == null && !Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING")){
            return;
        }

        // Initialize a new TripEventSnapshot and set the vehicle's current location.
        TripEventSnapshot tripEventSnapshot = new TripEventSnapshot();
        LatLong point = new LatLong(lat,lon);

        // End of speeding event condition: Check if the last status was "SPEEDING" and the current status is no longer "SPEEDING".
        if(Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING") && (speedLimitResponse == null || !Objects.equals(status, "SPEEDING")) && !tripEventMessage.getEventList().isEmpty()){
            // Retrieve the last event, set its end location and time, and calculate speeding duration.
            tripEventSnapshot = tripEventMessage.getEventList().get(tripEventMessage.getEventList().size()-1);
            tripEventSnapshot.setEndLocation(point);
            tripEventSnapshot.setEndTime(convertDate(dataItem.getGps().getDt()));
            tripEventSnapshot.setSpeedingTimeMs(returnTimeInSeconds(tripEventSnapshot.getEndTime(),1, dateFormatConverted) - returnTimeInSeconds(tripEventSnapshot.getStartTime(),1, dateFormatConverted));

            // Update the event list with the modified snapshot and reset the last status.
            tripEventMessage.getEventList().remove(tripEventMessage.getEventList().size()-1);
            tripEventMessage.getEventList().add(tripEventSnapshot);
            tripEventMessage.setLastStatus("");
            tripGeoMessage.getComputedValues().increaseSpeedViolations();
            return;
        }

        // If the vehicle is still speeding, update the max speed if the current speed exceeds previous max speed.
        else if (Objects.equals(tripEventMessage.getLastStatus(),"SPEEDING") && Objects.equals(status, "SPEEDING")) {
            if(!tripEventMessage.getEventList().isEmpty() && dataItem.getGps().getSp() > tripEventMessage.getEventList().get(tripEventMessage.getEventList().size()-1).getMaxSpeed()){
                tripEventMessage.getEventList().get(tripEventMessage.getEventList().size()-1).setMaxSpeed(dataItem.getGps().getSp());//
            }
            
        }

        // If there is no speed limit response or the current status is not "SPEEDING," exit the method.
        if(speedLimitResponse == null || !Objects.equals(status, "SPEEDING")){
            return;
        }


        if (!tripEventMessage.getEventList().isEmpty() &&
                Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING")) {
            return; // Skip adding a new event if already speeding.
        }


        // Start of a new speeding event: initialize the snapshot properties for a "speeding" event.
        tripEventSnapshot.setType("speeding");

        tripEventSnapshot.setAssetSpeedLimit(0);
        tripEventSnapshot.getEvent().setName("speeding");
        tripEventSnapshot.getEvent().setType("startend");

        tripEventSnapshot.setId(UUID.randomUUID().toString());
        tripEventSnapshot.setMaxSpeed(dataItem.getGps().getSp());
        tripEventSnapshot.setStartLocation(point);

        // Set the start time and timestamp for the event.
        String dt = dataItem.getGps().getDt();
        dt = convertDate(dt);
        tripEventSnapshot.setStartTime(dt);
        tripEventSnapshot.setTimestamp(dt);
        tripEventSnapshot.setTimestamp_millis(returnTimeInSeconds(dt,1, dateFormatConverted));
        tripEventSnapshot.setRoadSpeedLimit(speedLimitResponse.getMaxSpeed());

        // Check if the vehicle is within a geofence and set the start address if applicable.
        List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(server, metadata.getOrgId(),point,metadata.getSiteIds());
        if(geofenceItems!= null && !geofenceItems.isEmpty()){
            tripEventSnapshot.setStartAddress(geofenceItems.get(0).getName());
        }

        // Add the newly created speeding event snapshot to the event list.
        tripEventMessage.getEventList().add(tripEventSnapshot);
    }


    /**
     * Collects trip data and processes trip and event summaries based on the provided inputs.
     *
     * <p>This method sets the start and end points for a trip, updates computed values, checks for critical alerts,
     * and processes speeding events if necessary. It then generates and collects trip summary and event summary responses.</p>
     *
     * @param tripGeoMessage      The trip geolocation message that holds geographical data for the trip.
     * @param rawDataItem         The data item containing raw GPS data for time, speed, and location.
     * @param rawData             The complete raw data, including metadata about the organization and site.
     * @param lat                 The latitude of the current location of the vehicle.
     * @param lon                 The longitude of the current location of the vehicle.
     * @param status              The status of the trip, such as active or completed.
     * @param tripEventMessage    The event message object that stores trip events, such as speeding.
     * @param speedLimit          The speed limit response data, or {@code null} if no speed limit is available.
     * @param serial              The unique serial identifier for the vehicle or trip.
     * @param collector           The collector used to output processed trip summary and event summary data.
     *
     * @throws ParseException If there is an error parsing date strings from the {@link DataItem}.
     */
    public void collectTrip(TripGeoMessage tripGeoMessage, DataItem rawDataItem, ParagonRawData rawData, double lat, double lon, String status, TripEventMessage tripEventMessage, SpeedLimitResponse speedLimit, String serial, Collector<TripTopicResponse> collector) throws ParseException {

        // Set the end event in the tripGeoMessage using the current location and metadata.
        tripGeoMessage.setEndEvent(setStartAndEndEvent(rawDataItem, rawData.getMetadata().getOrgId(), rawData.getMetadata().getSiteIds(), lat, lon));

        // Update computed values such as speed, time, and status in tripGeoMessage.
        updateComputedValues(tripGeoMessage, rawDataItem.getGps().getSp(),returnTimeInSeconds(rawDataItem.getGps().getDt(),1, dateFormatOriginal),status,lat,lon, false);

        // Check for any critical alerts based on the current trip and raw data.
        checkCritical(tripGeoMessage, rawDataItem);

        // If the last status was "SPEEDING", update the event data to reflect the speeding event details.
        if(Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING")){
            setEventData(rawDataItem,tripEventMessage,speedLimit,"",lat,lon, config.getDatacacheServer(), rawData.getMetadata(), tripGeoMessage);
        }

        Logger.log("Ending trip: ", 5556);
        Logger.log("EventData: " + tripEventMessage.toString(), 5556);
        Logger.log("GeoData: " + tripGeoMessage.toString(), 5556);

        // Prepare the response object for trip topic and event summaries.
        TripTopicResponse response = new TripTopicResponse();
        Metadata metadata = new Metadata();
        metadata.setSerial(serial);
        metadata.setApp("tps");

        // If there are events in the event list, generate a trip event summary and collect it.
        if(tripEventMessage.getEventList() != null && !tripEventMessage.getEventList().isEmpty()){
            metadata.setType("trip_event_summary");
            response.setMeta(metadata);
            response.setPayload(tripEventMessage);

            // Link the event summary ID to the trip and collect the response.
            tripGeoMessage.setEventSummaryId(tripEventMessage.getId());
            collector.collect(response);
        }

        // Set the payload for the trip summary response with geographical trip data.
        response.setPayload(tripGeoMessage);

        // Set metadata type to trip summary and collect the response.
        metadata.setType("trip_summary");
        response.setMeta(metadata);

        // Collect the final trip summary data.
        collector.collect(response);
    }

    /**
     * Processes raw data from a vehicle and manages trip and event messages for geolocation and event handling.
     *
     * <p>This method retrieves or initializes trip and event message states for the specified vehicle,
     * processes each data item within the raw data, and determines the status of the trip, such as "MOVING",
     * "IDLE", or "SPEEDING". Based on conditions like trip start, continuation, and end, it updates trip details
     * and collects summaries as necessary.</p>
     *
     * @param rawData     The raw data containing GPS and metadata information from the vehicle.
     * @param collector   The collector used to output trip and event summary responses for downstream processing.
     *
     * @throws Exception If an error occurs during data parsing or processing.
     */
    @Override
    public void flatMap(ParagonRawData rawData, Collector<TripTopicResponse> collector) throws Exception {
        // Clean and standardize the serial identifier of the vehicle.
        String serial = Utils.cleanSerial(rawData.getSerial());

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

        // Process each data item in the raw data.
        for (DataItem rawDataItem : rawData.getData()) {

            // Skip items without valid GPS data
            if (rawDataItem.getGps().getFi() == 0) {
                continue;
            }

            // Parse latitude and longitude from the GPS data.
            double lat = Utils.parseLatitude(rawDataItem.getGps().getLa());
            double lon = Utils.parseLongitude(rawDataItem.getGps().getLo());

            // Fetch the speed limit for the current location, if available.
            SpeedLimitResponse speedLimit = Utils.getSpeedLimit(new SpeedLimitRequest(new LatLong(lat,lon)));

            // Determine the vehicle's status: "IDLE", "MOVING", or "SPEEDING".
            String status;
            if(rawDataItem.getGps().getSp() <= 5.0){
                status = "IDLE";
            }else{
                if(speedLimit != null && ((speedLimit.getMaxSpeed() + 10) < rawDataItem.getGps().getSp())){
                    status = "SPEEDING";
                }else {
                    status = "MOVING";
                }
            }

            long time = returnTimeInSeconds(rawDataItem.getGps().getDt(),1, dateFormatOriginal);

            if (tripGeoMessage.getLastTripId() == null && rawDataItem.getIg() == 1) {
                Logger.log("Starting trip for device: " + serial, 5556);

                long id;

                if(rawDataItem.getTid() != null){
                    id = rawDataItem.getTid();
                }else{
                    id = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                }
                String tripId = UUID.randomUUID() + "_" + id;

                tripGeoMessage.setLastTripId(id);
                setInitialTripMessage(tripGeoMessage, tripEventMessage, rawData, rawDataItem);

                tripGeoMessage.setSource("TRACKMATIC");

                tripGeoMessage.setStartEvent(setStartAndEndEvent(rawDataItem, rawData.getMetadata().getOrgId(), rawData.getMetadata().getSiteIds(), lat, lon));
                setPreviousDetails(tripGeoMessage,lat,lon, time);

                tripGeoMessage.setId(tripId);

                setEventData(rawDataItem,tripEventMessage,speedLimit,status,lat,lon, config.getDatacacheServer(), rawData.getMetadata(), tripGeoMessage);
                checkCritical(tripGeoMessage, rawDataItem);
                tripEventMessage.setLastStatus(status);
            }
            else if (tripGeoMessage.getLastTripId() != null && rawDataItem.getIg() != 0) {
                if (tripGeoMessage.getLastTime() < time) { //Make sure trip messages are always the next message and not any prior messages
                    TripGeoSnapshot tripGeoSnapshot = createGeoSnapshot(rawDataItem, lat, lon, rawData.getMetadata().getOrgId(), rawData.getMetadata().getSiteIds(),rawDataItem.getGps().getSp(), status);
                    tripGeoMessage.getSnapshots().add(tripGeoSnapshot);
                    updateComputedValues(tripGeoMessage, rawDataItem.getGps().getSp(),returnTimeInSeconds(rawDataItem.getGps().getDt(),1, dateFormatOriginal),status,lat,lon, true);
                    setPreviousDetails(tripGeoMessage,lat,lon,returnTimeInSeconds(rawDataItem.getGps().getDt(),1, dateFormatOriginal));
                    setEventData(rawDataItem,tripEventMessage,speedLimit,status,lat,lon, config.getDatacacheServer(), rawData.getMetadata(), tripGeoMessage);
                    checkCritical(tripGeoMessage, rawDataItem);
                    tripEventMessage.setLastStatus(status);
                }
            }
            // Handle trip ending conditions when ignition is off and priority flag is set.
            else if (rawDataItem.getIg() == 0 && tripGeoMessage.getLastTripId() != null) {//Trip ends
                collectTrip(tripGeoMessage,rawDataItem,rawData,lat,lon,status,tripEventMessage,speedLimit,serial,collector);

                tripGeoMessage = new TripGeoMessage();
                tripEventMessage = new TripEventMessage();
            }

            // Update the last known status of the trip event.

            // Store the updated trip and event message in the map state for this serial.
            mapState.put(serial, Tuple2.of(tripGeoMessage, tripEventMessage));
        }
    }
}
