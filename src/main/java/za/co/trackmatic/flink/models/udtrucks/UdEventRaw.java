package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a raw event from UD Trucks containing vehicle info, metadata,
 * a list of driving behavior events, and pagination details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UdEventRaw implements Serializable {

    private Vehicle vehicle;

    private TmMetadata metadata;

    private List<UdDrivingBehaviour> drivingBehaviourEvents;

    private Pagination pagination;


    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
    public Pagination getPagination() {
        return pagination;
    }

    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }

    public Vehicle getVehicle() {
        return vehicle;
    }

    public void setVehicle(Vehicle vehicle) {
        this.vehicle = vehicle;
    }

    public List<UdDrivingBehaviour> getDrivingBehaviourEvents() {
        return drivingBehaviourEvents;
    }

    public void setDrivingBehaviourEvents(List<UdDrivingBehaviour> drivingBehaviourEvents) {
        this.drivingBehaviourEvents = drivingBehaviourEvents;
    }
}
