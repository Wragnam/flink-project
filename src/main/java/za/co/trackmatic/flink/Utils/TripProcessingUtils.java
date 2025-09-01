package za.co.trackmatic.flink.Utils;

import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.models.mappingAPI.SpeedLimitResponse;
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.trips.*;

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

/**
 * Helper class, containing all functions that are used to create trips
 */
public class TripProcessingUtils {

    private static final String dateFormatConverted = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public TripProcessingUtils() {
    }

    /**
     * Determines the status of a vehicle based on its speed and a given speed limit.
     * <p>
     * The status is determined as follows:
     * <ul>
     *   <li>"IDLE" if speed is less than or equal to 5.0</li>
     *   <li>"SPEEDING" if speed exceeds the speed limit by more than 10 units</li>
     *   <li>"MOVING" otherwise</li>
     * </ul>
     *
     * @param speed       The current speed of the vehicle.
     * @param speedLimit  The speed limit response containing the max allowed speed; can be null.
     * @return A string representing the vehicle status: "IDLE", "SPEEDING", or "MOVING".
     */
    public static String getStatus(Double speed, SpeedLimitResponse speedLimit) {
        String status;
        if (speed <= 5.0) {
            status = "IDLE";
        } else {
            if (speedLimit != null && ((speedLimit.getMaxSpeed() + 10) < speed)) {
                status = "SPEEDING";
            } else {
                status = "MOVING";
            }
        }
        return status;
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
    public static List<String> getInitialsAndSurname(String fullName) {
        List<String> initialsAndSurname = new ArrayList<>();

        // Check if the input is null or empty, and return empty strings if so.
        if (fullName == null || fullName.isEmpty()) {
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
                if (names[i].isEmpty()) {
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
     * Sets the initial trip message by populating the `tripGeoMessage` and `tripEventMessage`
     * with relevant context and driver details based on the provided data.
     * <p>
     * This method sets the asset ID, load ID, device ID, site ID, organization ID, and trip ID
     * into the context of the trip messages. It also extracts and sets the driver's initials and
     * last name based on the provided driver name in the raw data.
     * </p>
     *
     * @param tripGeoMessage   The `TripGeoMessage` object to be populated with context and driver details.
     * @param tripEventMessage The `TripEventMessage` object to be populated with context and driver details.
     * @param metadata         The metadata and other information about the trip.
     * @param serial           The cleaned device's serial number.
     * @param tId              The current trip Id.
     */
    public static void setInitialTripMessage(TripGeoMessage tripGeoMessage, TripEventMessage tripEventMessage, TmMetadata metadata, String serial, long tId) {
        Context c = new Context();

        if (metadata.getActiveLoad() != null) {
            c.setLoadId(metadata.getActiveLoad().getLoadId());
            c.setSiteId(metadata.getActiveLoad().getSiteId());

            // Extract the initials and surname of the driver from the raw data.
            List<String> initAndSurname = getInitialsAndSurname(metadata.getActiveLoad().getDriverName());

            // Set the driver's initials and last name in the tripGeoMessage.
            tripGeoMessage.getDriverDetails().setInitials(initAndSurname.get(0));
            tripGeoMessage.getDriverDetails().setLastName(initAndSurname.get(1));
        }

        c.setAssetId(metadata.getAssetId());
        c.setDeviceId(serial);
        c.setOrgId(metadata.getOrgId());

        // Generate a unique trip ID by combining a UUID with the trip ID from the data item.
        String tripId = UUID.randomUUID() + "_" + tId;
        c.setTripId(tripId);

        // Set the context for both the tripGeoMessage and tripEventMessage.
        tripGeoMessage.setContext(c);
        tripEventMessage.setContext(c);
    }

    /**
     * Converts a date string from the original format to the specified converted format.
     * <p>
     * The method attempts to parse the given date string according to the predefined
     * `dateFormatOriginal`. If parsing fails, the current UTC date and time are used as a fallback.
     * </p>
     *
     * @param dateString          The date string to be converted, expected in `dateFormatOriginal`.
     * @param dateFormatOriginal  The format of the date as it comes from the raw data.
     * @return The formatted date string in `dateFormatConverted`.
     */
    public static String convertDate(String dateString, String dateFormatOriginal) {
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
        String dateFormatConverted = "yyyy-MM-dd'T'HH:mm:ss'Z'";
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
     * @param orgId             The organization ID for the geofence query.
     * @param siteIds           A list of site IDs for the geofence query.
     * @param lat               The latitude of the point to check.
     * @param lon               The longitude of the point to check.
     * @param datacacheServer   The server address of the datacache.
     * @return A LocationInfo object containing the ID and name of the first geofence found, or null if no geofence contains the point.
     */
    public static LocationInfo getLocation(String orgId, List<String> siteIds, double lat, double lon, String datacacheServer) {

        // Check if the given point (lat, lon) is inside any geofences for the given organization and site IDs.
        List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(datacacheServer, orgId, new LatLong(lat, lon), siteIds);

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
     * Sets the start and end event location information for a given data point.
     * <p>
     * This method creates an EventLocation object and populates it with the location information,
     * date-time, and coordinates (latitude and longitude) extracted from the provided raw data,
     * along with the organization and site IDs to determine the relevant location.
     * </p>
     *
     * @param orgId             The organization ID used to retrieve geofence information.
     * @param siteIds           A list of site IDs for geofence lookup.
     * @param lat               The latitude of the location to set.
     * @param lon               The longitude of the location to set.
     * @param date              The date the event took place
     * @param datacacheServer   The server address of the datacache.
     * @param dateFormat        The format of the date
     * @return The EventLocation object populated with location details, date-time, and coordinates.
     */
    public static EventLocation setStartAndEndEvent(String orgId, List<String> siteIds, double lat, double lon, String datacacheServer, String date, String dateFormat) {
        // Create a new EventLocation object to store event data.
        EventLocation eventLocation = new EventLocation();

        // Retrieve the location information based on the organization ID, site IDs, and coordinates.
        LocationInfo locationInfo = getLocation(orgId, siteIds, lat, lon, datacacheServer);

        // Set the location information to the event location object.
        eventLocation.setLocation(locationInfo);

        // Convert and set the date-time for the event using the raw data's GPS timestamp.
        eventLocation.setDateTime(convertDate(date, dateFormat));

        // Set the latitude and longitude of the event.
        eventLocation.setLat(lat);
        eventLocation.setLng(lon);

        // Return the populated EventLocation object.
        return eventLocation;
    }

    /**
     * Converts a formatted time string into seconds by parsing it with the specified format
     * and dividing the result by a given factor.
     *
     * @param time         The time as a String to be parsed.
     * @param divideFactor The factor by which to divide the parsed time in milliseconds
     *                     to convert it into seconds (e.g., 1000 to convert milliseconds to seconds).
     * @param format       The format of the input time String (e.g., "HH:mm:ss").
     * @return The time in seconds, calculated by parsing the time and dividing by the factor.
     * @throws ParseException If the time string cannot be parsed with the provided format.
     */
    public static long returnTimeInSeconds(String time, int divideFactor, String format) throws ParseException {
        SimpleDateFormat dateParser = new SimpleDateFormat(format);
        return dateParser.parse(time).getTime() / divideFactor;
    }

    /**
     * Sets the previous location and time details for a given trip.
     * This method updates the last known latitude, longitude, and timestamp for the trip,
     * which can be used to calculate distance and time intervals in subsequent updates.
     *
     * @param tripGeoMessage The TripGeoMessage object containing the trip details.
     * @param lat            The latitude of the previous location.
     * @param lon            The longitude of the previous location.
     * @param time           The timestamp of the previous location in milliseconds.
     */
    public static void setPreviousDetails(TripGeoMessage tripGeoMessage, double lat, double lon, long time) {

        // Sets the last known latitude of the trip.
        tripGeoMessage.setLastLat(lat);

        // Sets the last known longitude of the trip.
        tripGeoMessage.setLastLon(lon);

        // Sets the last recorded timestamp for the trip.
        tripGeoMessage.setLastTime(time);
    }

    /**
     * Sets the event data for a trip, specifically handling speeding events based on the current status,
     * speed limit response, and location. This method creates or updates event snapshots depending on
     * the current and last recorded statuses and adds snapshots to the trip event message.
     *
     * @param tripEventMessage   the message containing trip events and last status.
     * @param speedLimitResponse the response containing the speed limit data for the current location.
     * @param status             the current trip status (e.g., "SPEEDING" or "MOVING").
     * @param lat                the latitude of the current location.
     * @param lon                the longitude of the current location.
     * @param server             the server used to check geofence membership.
     * @param metadata           metadata for the trip, including organization and site details.
     * @param tripGeoMessage     the message containing geographical data and computed values for the trip.
     * @param date               the date that the event took place
     * @param dateFormat         the format that the date is in
     * @param speed              the speed of the vehicle
     * @throws ParseException if there is an error parsing date or time values.
     */
    public static void setEventData(TripEventMessage tripEventMessage, SpeedLimitResponse speedLimitResponse, String status, double lat, double lon, String server, TmMetadata metadata, TripGeoMessage tripGeoMessage, String date, String dateFormat, Double speed) throws ParseException {
        // If no speed limit response is available and the last status was not speeding, exit the method.
        if (speedLimitResponse == null && !Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING")) {
            return;
        }

        // Initialize a new TripEventSnapshot and set the vehicle's current location.
        TripEventSnapshot tripEventSnapshot = new TripEventSnapshot();
        LatLong point = new LatLong(lat, lon);

        // End of speeding event condition: Check if the last status was "SPEEDING" and the current status is no longer "SPEEDING".
        if (Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING") && (speedLimitResponse == null || !Objects.equals(status, "SPEEDING")) && !tripEventMessage.getEventList().isEmpty()) {
            // Retrieve the last event, set its end location and time, and calculate speeding duration.
            tripEventSnapshot = tripEventMessage.getEventList().get(tripEventMessage.getEventList().size() - 1);
            tripEventSnapshot.setEndLocation(point);
            tripEventSnapshot.setEndTime(convertDate(date, dateFormat));
            tripEventSnapshot.setSpeedingTimeMs(returnTimeInSeconds(tripEventSnapshot.getEndTime(), 1, dateFormatConverted) - returnTimeInSeconds(tripEventSnapshot.getStartTime(), 1, dateFormatConverted));

            // Update the event list with the modified snapshot and reset the last status.
            tripEventMessage.getEventList().remove(tripEventMessage.getEventList().size() - 1);
            tripEventMessage.getEventList().add(tripEventSnapshot);
            tripEventMessage.setLastStatus("");
            tripGeoMessage.getComputedValues().increaseSpeedViolations();
            return;
        }

        // If the vehicle is still speeding, update the max speed if the current speed exceeds previous max speed.
        else if (Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING") && Objects.equals(status, "SPEEDING")) {
            if (!tripEventMessage.getEventList().isEmpty() && speed > tripEventMessage.getEventList().get(tripEventMessage.getEventList().size() - 1).getMaxSpeed()) {
                tripEventMessage.getEventList().get(tripEventMessage.getEventList().size() - 1).setMaxSpeed(speed);//
            }

        }

        // If there is no speed limit response or the current status is not "SPEEDING," exit the method.
        if (speedLimitResponse == null || !Objects.equals(status, "SPEEDING")) {
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
        tripEventSnapshot.setMaxSpeed(speed);
        tripEventSnapshot.setStartLocation(point);

        // Set the start time and timestamp for the event.
        String dt = date;
        dt = convertDate(dt, dateFormat);
        tripEventSnapshot.setStartTime(dt);
        tripEventSnapshot.setTimestamp(dt);
        tripEventSnapshot.setTimestamp_millis(returnTimeInSeconds(dt, 1, dateFormatConverted));
        tripEventSnapshot.setRoadSpeedLimit(speedLimitResponse.getMaxSpeed());

        // Check if the vehicle is within a geofence and set the start address if applicable.
        List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(server, metadata.getOrgId(), point, metadata.getSiteIds());
        if (geofenceItems != null && !geofenceItems.isEmpty()) {
            tripEventSnapshot.setStartAddress(geofenceItems.get(0).getName());
        }

        // Add the newly created speeding event snapshot to the event list.
        tripEventMessage.getEventList().add(tripEventSnapshot);
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
     * @param lat               The latitude of the geographical location to set in the snapshot.
     * @param lon               The longitude of the geographical location to set in the snapshot.
     * @param orgId             The organization ID used to retrieve geofence information.
     * @param siteIds           A list of site IDs for geofence lookup.
     * @param speed             The speed at the time of the snapshot.
     * @param status            The current status of the trip (e.g., "MOVING", "IDLE").
     * @param accuracy          The accuracy of the gps point (nullable).
     * @param date              The date string.
     * @param dateFormat        The format of the date from the raw data.
     * @param heading           The direction in which the asset is moving.
     * @param datacacheServer   The server address of the datacache.
     * @param numSat            The amount of satellites used to get the gps point (nullable).
     * @param batteryLevel      The level of the battery.
     * @return A TripGeoSnapshot object populated with the geographical and sensor data.
     */
    public static TripGeoSnapshot createGeoSnapshot(double lat, double lon, String orgId, List<String> siteIds,
                                                    double speed, String status, Double accuracy, String date,
                                                    String dateFormat, Double heading,
                                                    String datacacheServer, Integer numSat, Integer batteryLevel) {

        // Create a new TripGeoSnapshot object to store the geographical and sensor data.
        TripGeoSnapshot tripGeoSnapshot = new TripGeoSnapshot();

        tripGeoSnapshot.setAccuracy(accuracy);
        tripGeoSnapshot.setDateTime(convertDate(date, dateFormat));
        tripGeoSnapshot.setDirection(heading);
        tripGeoSnapshot.setLat(lat);
        tripGeoSnapshot.setLng(lon);
        tripGeoSnapshot.setLocation(getLocation(orgId, siteIds, lat, lon, datacacheServer));
        tripGeoSnapshot.getOptional().setSat(numSat);
        tripGeoSnapshot.setSpeed(speed);
        tripGeoSnapshot.setStatus(status);


        tripGeoSnapshot.setBatteryLevel(batteryLevel);


        // Return the populated TripGeoSnapshot object.
        return tripGeoSnapshot;
    }

    public static boolean datesEqual(String date, String dateFormat, String prevDate) throws ParseException {
        long currTime = returnTimeInSeconds(date, 1000, dateFormat);
        long prevTime = returnTimeInSeconds(prevDate, 1000, dateFormatConverted);

        return currTime >= prevTime + 60;
    }

    /**
     * Updates the computed values of a trip based on the provided parameters, including
     * speed, time, status, and location. It adjusts the total speed, steps, average speed,
     * maximum speed, driving and idle times, total time, and distance.
     *
     * @param tripGeoMessage the message containing trip details and computed values.
     * @param speed          the current speed at this point in the trip.
     * @param time           the timestamp for the current update.
     * @param status         the status of the trip (e.g., "MOVING" or "IDLE").
     * @param lat            the latitude of the current position.
     * @param lon            the longitude of the current position.
     * @param incrementSteps a boolean indicating whether to increment the step count.
     */
    public static void updateComputedValues(TripGeoMessage tripGeoMessage, double speed, long time, String status, double lat, double lon, boolean incrementSteps) {
        // Increases the total speed sum by the current speed.
        tripGeoMessage.increaseTotalSpeed(speed);

        // Increments the step count if incrementSteps is true.
        if (incrementSteps) {
            tripGeoMessage.incrementSteps();
        }

        // Calculates and updates the average speed based on total speed and steps.
        tripGeoMessage.getComputedValues().setAverageSpeed(tripGeoMessage.getTotalSpeed() / tripGeoMessage.getSteps());

        // Updates the maximum speed if the current speed exceeds the previously recorded maximum speed.
        if (speed > tripGeoMessage.getComputedValues().getMaxSpeed()) {
            tripGeoMessage.getComputedValues().setMaxSpeed(speed);
        }

        // Calculates time difference and updates driving or idle time based on status.
        if (tripGeoMessage.getLastTime() != null) {
            long timeDifference = time - tripGeoMessage.getLastTime();
            if (status.equals("MOVING") || status.equals("SPEEDING")) {
                tripGeoMessage.getComputedValues().incrementDrivingTime(timeDifference);
            } else {
                tripGeoMessage.getComputedValues().incrementIdleTime(timeDifference);
            }
        }

        // Updates the total time by summing driving and idle times.
        tripGeoMessage.getComputedValues().setTotalTime(tripGeoMessage.getComputedValues().getDrivingTime() + tripGeoMessage.getComputedValues().getIdleTime());

        // Calculates and increments the total distance using the last known position, if available.
        if (tripGeoMessage.getLastLat() != null && tripGeoMessage.getLastLon() != null) {
            tripGeoMessage.getComputedValues().incrementTotalDistance(Utils.getDistanceBetweenPoints(new LatLong(tripGeoMessage.getLastLat(), tripGeoMessage.getLastLon()), new LatLong(lat, lon)) * 1000);
        }

    }

    /**
     * Collects trip data and processes trip and event summaries based on the provided inputs.
     *
     * <p>This method sets the start and end points for a trip, updates computed values, checks for critical alerts,
     * and processes speeding events if necessary. It then generates and collects trip summary and event summary responses.</p>
     *
     * @param tripGeoMessage   The trip geolocation message that holds geographical data for the trip.
     * @param lat              The latitude of the current location of the vehicle.
     * @param lon              The longitude of the current location of the vehicle.
     * @param status           The status of the trip, such as active or completed.
     * @param tripEventMessage The event message object that stores trip events, such as speeding.
     * @param speedLimit       The speed limit response data, or {@code null} if no speed limit is available.
     * @param serial           The unique serial identifier for the vehicle or trip.
     * @param collector        The collector used to output processed trip summary and event summary data.
     * @param metadata         The metadata of the asset.
     * @param server           The server of the datacache.
     * @param date             The date associated to the datapoint.
     * @param dateFormat       The format of the date from the raw data.
     * @param speed            The speed at which the asset is moving.
     * @throws ParseException If there is an error parsing date strings from the {@link DataItem}.
     */
    public static void collectTrip(TripGeoMessage tripGeoMessage, double lat, double lon, String status, TripEventMessage tripEventMessage, SpeedLimitResponse speedLimit, String serial, Collector<TripTopicResponse> collector, TmMetadata metadata, String server, String date, String dateFormat, double speed) throws ParseException {

        // Set the end event in the tripGeoMessage using the current location and metadata.
        tripGeoMessage.setEndEvent(setStartAndEndEvent(metadata.getOrgId(), metadata.getSiteIds(), lat, lon, server, date, dateFormat));

        // Update computed values such as speed, time, and status in tripGeoMessage.
        updateComputedValues(tripGeoMessage, speed, returnTimeInSeconds(date, 1, dateFormat), status, lat, lon, false);

        // Check for any critical alerts based on the current trip and raw data.
//        checkCritical(tripGeoMessage, rawDataItem);

        // If the last status was "SPEEDING", update the event data to reflect the speeding event details.
        if (Objects.equals(tripEventMessage.getLastStatus(), "SPEEDING")) {
            setEventData(tripEventMessage, speedLimit, "", lat, lon, server, metadata, tripGeoMessage, date, dateFormat, speed);
        }

        Logger.log("Ending trip: ", 5600);
        Logger.log("EventData: " + tripEventMessage.toString(), 5600);
        Logger.log("GeoData: " + tripGeoMessage.toString(), 5600);

        // Prepare the response object for trip topic and event summaries.
        TripTopicResponse response = new TripTopicResponse();
        Metadata respMetadata = new Metadata();
        respMetadata.setSerial(serial);
        respMetadata.setApp("tps");

        // If there are events in the event list, generate a trip event summary and collect it.
        if (tripEventMessage.getEventList() != null && !tripEventMessage.getEventList().isEmpty()) {
            respMetadata.setType("trip_event_summary");
            response.setMeta(respMetadata);
            response.setPayload(tripEventMessage);

            // Link the event summary ID to the trip and collect the response.
            tripGeoMessage.setEventSummaryId(tripEventMessage.getId());
            collector.collect(response);
        }

        // Set the payload for the trip summary response with geographical trip data.
        response.setPayload(tripGeoMessage);

        // Set metadata type to trip summary and collect the response.
        respMetadata.setType("trip_summary");
        response.setMeta(respMetadata);

        // Collect the final trip summary data.
        collector.collect(response);
    }
}
