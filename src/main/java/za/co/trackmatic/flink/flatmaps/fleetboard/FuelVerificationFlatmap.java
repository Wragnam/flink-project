package za.co.trackmatic.flink.flatmaps.fleetboard;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.fleetboard.FuelConfirmation;
import za.co.trackmatic.flink.models.fleetboard.FuelVerificationData;
import za.co.trackmatic.flink.models.fleetboard.Position;
import za.co.trackmatic.flink.models.fleetboard.PositionTrace;
import za.co.trackmatic.flink.models.trackmatic.LatLong;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * A Flink {@link RichFlatMapFunction} that processes {@link FuelVerificationData} inputs
 * to generate {@link FuelConfirmation} outputs.
 *
 * <p>This class computes the distances between actual GPS coordinates and reported
 * vehicle positions or trace data, confirming fuel positions based on proximity
 * thresholds. It prioritizes trace data if it is closer than the position data.
 *
 * <p>The class also handles timestamp conversions and applies a fixed time zone adjustment.
 */
public class FuelVerificationFlatmap extends RichFlatMapFunction<FuelVerificationData, FuelConfirmation> implements Serializable {

    /**
     * Sets the position data for a FuelConfirmation object based on the provided Position and distance.
     * Updates the latitude, longitude, distance, and timestamp fields of the confirmation.
     *
     * @param confirmation The FuelConfirmation object to update with position data.
     * @param pos The Position object containing latitude, longitude, and timestamp information.
     * @param distance The calculated distance to set in the confirmation.
     * @return The updated FuelConfirmation object with the new position data.
     */
    public FuelConfirmation setPositionData(FuelConfirmation confirmation, Position pos, double distance){
        confirmation.setVehicleLat(pos.getLat());
        confirmation.setVehicleLon(pos.getLon());
        confirmation.setDistanceBetween(distance);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dtTime = LocalDateTime.parse(pos.getTimestamp(),format).plusHours(2);
        confirmation.setTimeStamp(dtTime.toString());
        return confirmation;
    }

    /**
     * Sets trace data for a FuelConfirmation object using PositionTrace information.
     * Updates the latitude, longitude, distance, and timestamp fields of the confirmation.
     *
     * @param confirmation The FuelConfirmation object to update with trace data.
     * @param trace The PositionTrace object containing latitude, longitude, and GPS time information.
     * @param distance The calculated distance to set in the confirmation.
     * @return The updated FuelConfirmation object with the new trace data.
     */
    public FuelConfirmation setTraceData(FuelConfirmation confirmation, PositionTrace trace, double distance){
        confirmation.setVehicleLat(trace.getLat());
        confirmation.setVehicleLon(trace.getLon());
        confirmation.setDistanceBetween(distance);

        // Define the expected date format
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Parse GPS time from trace, adjust by 2 hours, and set as confirmation timestamp
        LocalDateTime dtTime = LocalDateTime.parse(trace.getGpsTime(), format).plusHours(2);
        confirmation.setTimeStamp(dtTime.toString());
        return confirmation;
    }

    /**
     * Processes FuelVerificationData to create and emit a FuelConfirmation.
     * Computes distances between the actual location and provided position/trace data,
     * setting confirmation based on proximity and trace or position data priority.
     *
     * @param fuelData The FuelVerificationData containing actual, position, and trace location details.
     * @param collector The collector to emit the generated FuelConfirmation object.
     * @throws Exception If an error occurs during distance calculations or data collection.
     */
    @Override
    public void flatMap(FuelVerificationData fuelData, Collector<FuelConfirmation> collector) throws Exception {

        Position fuelDataPos = fuelData.getPosition();
        PositionTrace fuelDataPosTrace = fuelData.getTrace();
        double exactLat = fuelData.getLat();
        double exactLon = fuelData.getLon();

        // Actual location from fuel data
        LatLong actualLocation = new LatLong(exactLat,exactLon);
        double positionDistance = Double.MAX_VALUE;
        double traceDistance = Double.MAX_VALUE;

        // Calculate distance to vehicle position if available
        if (!Objects.equals(fuelDataPos.getVehicleID(), "")){
            LatLong positionLocation = new LatLong(fuelDataPos.getLat(),fuelDataPos.getLon());
            positionDistance = Utils.getDistanceBetweenPoints(actualLocation,positionLocation);
        }

        // Calculate distance to trace location if available
        if (!Objects.equals(fuelDataPosTrace.getVehicleId(), "")) {
            LatLong traceLocation = new LatLong(fuelDataPosTrace.getLat(), fuelDataPosTrace.getLon());
            traceDistance = Utils.getDistanceBetweenPoints(actualLocation,traceLocation);
        }

        // Initialize a new FuelConfirmation object
        FuelConfirmation confirmation = new FuelConfirmation();

        confirmation.setSource("FLEETBOARD");
        confirmation.setExactLat(exactLat);
        confirmation.setExactLon(exactLon);

        // If position distance is within threshold, prioritize position and trace data
        if (positionDistance < 1000){
            confirmation.setConfirmedPosition(true);

            // Use trace data if it's closer; otherwise, use position data
            if (traceDistance < positionDistance){
                confirmation = setTraceData(confirmation, fuelDataPosTrace, traceDistance);
            }
            else{
                confirmation = setPositionData(confirmation, fuelDataPos, positionDistance);
            }
        }
        else{
            // If position distance is not within threshold, evaluate trace data
            if (traceDistance < 1000){
                confirmation.setConfirmedPosition(true);
                confirmation = setTraceData(confirmation, fuelDataPosTrace, traceDistance);
            }else{
                confirmation.setConfirmedPosition(false);
                // Set data based on whichever is closer: trace or position
                if (traceDistance < positionDistance){
                    confirmation = setTraceData(confirmation, fuelDataPosTrace, traceDistance);
                }
                else{
                    confirmation = setPositionData(confirmation,fuelDataPos,positionDistance);
                }
            }
        }
        Logger.log(confirmation.toString(), 5559);
        collector.collect(confirmation);
    }
}
