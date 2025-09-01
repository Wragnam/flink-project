package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents computed metrics for a vehicle trip.
 * <p>
 * This includes information such as average speed, idle time, driving time,
 * speed violations, and total distance traveled.
 * </p>
 */
public class ComputedTripValues implements Serializable {

    /**
     * The average speed of the vehicle during the trip.
     */
    private double averageSpeed;

    /**
     * Indicates if a critical low violation was successfully avoided during the trip.
     */
    private boolean criticalLowAvoided;

    /**
     * Total driving time in milliseconds.
     */
    private long drivingTime;

    /**
     * Total idle time in milliseconds.
     */
    private long idleTime;

    /**
     * The maximum speed recorded during the trip.
     */
    private double maxSpeed;

    /**
     * The number of speed violations recorded during the trip.
     */
    private int speedViolations;

    /**
     * Total duration of the trip in milliseconds.
     */
    private long totalTime;

    /**
     * Total distance traveled in meters.
     */
    private double totalDistance;

    public double getTotalDistance() {
        return totalDistance;
    }

    public void setTotalDistance(double totalDistance) {
        this.totalDistance = totalDistance;
    }

    public void incrementTotalDistance(double distance){
        this.totalDistance += distance;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public long getDrivingTime() {
        return drivingTime;
    }

    public void setDrivingTime(long drivingTime) {
        this.drivingTime = drivingTime;
    }

    public void incrementDrivingTime(long drivingTime){
        this.drivingTime += drivingTime;
    }

    public long getIdleTime() {
        return idleTime;
    }

    public void setIdleTime(long idleTime) {
        this.idleTime = idleTime;
    }

    public void incrementIdleTime(long idleTime){
        this.idleTime += idleTime;
    }

    public double getMaxSpeed() {
        return maxSpeed;
    }

    public void setMaxSpeed(double maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public int getSpeedViolations() {
        return speedViolations;
    }

    public void setSpeedViolations(int speedViolations) {
        this.speedViolations = speedViolations;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public void increaseTotalTime(long time){
        this.totalTime += time;
    }

    public void increaseSpeedViolations(){
        this.speedViolations++;
    }

    public boolean isCriticalLowAvoided() {
        return criticalLowAvoided;
    }

    public void setCriticalLowAvoided(boolean criticalLowAvoided) {
        this.criticalLowAvoided = criticalLowAvoided;
    }

    /**
     * Constructs a new {@code ComputedTripValues} object with the specified values.
     *
     * @param averageSpeed     the average speed
     * @param drivingTime      the total driving time
     * @param idleTime         the total idle time
     * @param maxSpeed         the maximum speed
     * @param speedViolations  the number of speed violations
     * @param totalTime        the total time of the trip
     * @param totalDistance    the total distance traveled
     */
    public ComputedTripValues(double averageSpeed, long drivingTime, long idleTime, double maxSpeed, int speedViolations, long totalTime, double totalDistance) {
        this.averageSpeed = averageSpeed;
        this.drivingTime = drivingTime;
        this.idleTime = idleTime;
        this.maxSpeed = maxSpeed;
        this.speedViolations = speedViolations;
        this.totalTime = totalTime;
        this.totalDistance = totalDistance;
        this.criticalLowAvoided = true;
    }


}
