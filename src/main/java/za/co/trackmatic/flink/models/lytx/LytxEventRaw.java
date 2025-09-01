package za.co.trackmatic.flink.models.lytx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the raw event data from the Lytx system.
 * <p>
 * This class contains metadata and sensor readings associated with a triggered Lytx event,
 * such as speeding, harsh braking, or coaching activities. It also includes contextual data
 * like driver, coach, vehicle information, and various thresholds and measurements.
 * </p>
 *
 * <p>
 * Unknown JSON fields are ignored during deserialization.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LytxEventRaw implements Serializable {

    private String eventId;

    private String id;

    private String customerEventId;

    private Integer eventTriggerId;

    private Integer eventStatusId;

    private String recordDateUTC;

    private String recordDateTZ;

    private Integer recordDateUT;

    private String downloadedDate;

    private Integer score;

    private String reviewedDate;

    private String erSerialNumber;

    private double overDue;

    private String vehicleId;

    private String groupId;

    private double forwardMax;

    private double lateralMax;

    private double forwardThreshold;

    private double lateralThreshold;

    private double shockThreshold;

    private double speed;

    private double latitude;

    private double longitude;

    private double heading;

    private String driverId;

    private String coachId;

    private String coachedDate;

    private String creationDate;

    private Object notes;

    private List<Behaviors> behaviors;

    private Integer objectRevision;

    private String revisionDate;

    private String coachEmployeeNum;

    private String coachFirstName;

    private String coachLastName;

    private String driverEmployeeNum;

    private String driverFirstName;

    private String driverLastName;

    private String coachingOverdueDate;

    private TmMetadata metadata;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCustomerEventId() {
        return customerEventId;
    }

    public void setCustomerEventId(String customerEventId) {
        this.customerEventId = customerEventId;
    }

    public Integer getEventTriggerId() {
        return eventTriggerId;
    }

    public void setEventTriggerId(Integer eventTriggerId) {
        this.eventTriggerId = eventTriggerId;
    }

    public Integer getEventStatusId() {
        return eventStatusId;
    }

    public void setEventStatusId(Integer eventStatusId) {
        this.eventStatusId = eventStatusId;
    }

    public String getRecordDateUTC() {
        return recordDateUTC;
    }

    public void setRecordDateUTC(String recordDateUTC) {
        this.recordDateUTC = recordDateUTC;
    }

    public String getRecordDateTZ() {
        return recordDateTZ;
    }

    public void setRecordDateTZ(String recordDateTZ) {
        this.recordDateTZ = recordDateTZ;
    }

    public Integer getRecordDateUT() {
        return recordDateUT;
    }

    public void setRecordDateUT(Integer recordDateUT) {
        this.recordDateUT = recordDateUT;
    }

    public String getDownloadedDate() {
        return downloadedDate;
    }

    public void setDownloadedDate(String downloadedDate) {
        this.downloadedDate = downloadedDate;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public String getReviewedDate() {
        return reviewedDate;
    }

    public void setReviewedDate(String reviewedDate) {
        this.reviewedDate = reviewedDate;
    }

    public String getErSerialNumber() {
        return erSerialNumber;
    }

    public void setErSerialNumber(String erSerialNumber) {
        this.erSerialNumber = erSerialNumber;
    }

    public double getOverDue() {
        return overDue;
    }

    public void setOverDue(double overDue) {
        this.overDue = overDue;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public double getForwardMax() {
        return forwardMax;
    }

    public void setForwardMax(double forwardMax) {
        this.forwardMax = forwardMax;
    }

    public double getLateralMax() {
        return lateralMax;
    }

    public void setLateralMax(double lateralMax) {
        this.lateralMax = lateralMax;
    }

    public double getForwardThreshold() {
        return forwardThreshold;
    }

    public void setForwardThreshold(double forwardThreshold) {
        this.forwardThreshold = forwardThreshold;
    }

    public double getLateralThreshold() {
        return lateralThreshold;
    }

    public void setLateralThreshold(double lateralThreshold) {
        this.lateralThreshold = lateralThreshold;
    }

    public double getShockThreshold() {
        return shockThreshold;
    }

    public void setShockThreshold(double shockThreshold) {
        this.shockThreshold = shockThreshold;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getHeading() {
        return heading;
    }

    public void setHeading(double heading) {
        this.heading = heading;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public String getCoachId() {
        return coachId;
    }

    public void setCoachId(String coachId) {
        this.coachId = coachId;
    }

    public String getCoachedDate() {
        return coachedDate;
    }

    public void setCoachedDate(String coachedDate) {
        this.coachedDate = coachedDate;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public Object getNotes() {
        return notes;
    }

    public void setNotes(Object notes) {
        this.notes = notes;
    }

    public List<Behaviors> getBehaviors() {
        return behaviors;
    }

    public void setBehaviors(List<Behaviors> behaviors) {
        this.behaviors = behaviors;
    }

    public Integer getObjectRevision() {
        return objectRevision;
    }

    public void setObjectRevision(Integer objectRevision) {
        this.objectRevision = objectRevision;
    }

    public String getRevisionDate() {
        return revisionDate;
    }

    public void setRevisionDate(String revisionDate) {
        this.revisionDate = revisionDate;
    }

    public String getCoachEmployeeNum() {
        return coachEmployeeNum;
    }

    public void setCoachEmployeeNum(String coachEmployeeNum) {
        this.coachEmployeeNum = coachEmployeeNum;
    }

    public String getCoachFirstName() {
        return coachFirstName;
    }

    public void setCoachFirstName(String coachFirstName) {
        this.coachFirstName = coachFirstName;
    }

    public String getCoachLastName() {
        return coachLastName;
    }

    public void setCoachLastName(String coachLastName) {
        this.coachLastName = coachLastName;
    }

    public String getDriverEmployeeNum() {
        return driverEmployeeNum;
    }

    public void setDriverEmployeeNum(String driverEmployeeNum) {
        this.driverEmployeeNum = driverEmployeeNum;
    }

    public String getDriverFirstName() {
        return driverFirstName;
    }

    public void setDriverFirstName(String driverFirstName) {
        this.driverFirstName = driverFirstName;
    }

    public String getDriverLastName() {
        return driverLastName;
    }

    public void setDriverLastName(String driverLastName) {
        this.driverLastName = driverLastName;
    }

    public String getCoachingOverdueDate() {
        return coachingOverdueDate;
    }

    public void setCoachingOverdueDate(String coachingOverdueDate) {
        this.coachingOverdueDate = coachingOverdueDate;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
