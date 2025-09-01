package za.co.trackmatic.flink.models.mappingAPI;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration parameters for route mapping and optimization.
 *
 * <p>This class holds various settings used to configure routing,
 * such as routing type, speed limits, road avoidance options,
 * optimization mode, and truck-specific constraints.</p>
 */
public class MappingConfig implements Serializable {

    public MappingConfig(){}

    public MappingConfig(String orgId){
        this.routingType = "CAR";
        this.maxSpeedInKmPerHour = 120;
        this.optimizationMode="FASTEST";
        this.roadAvoidanceFeatures = new ArrayList<>();
        this.orgId = orgId;
        this.truckConstraints = new TruckConstraints();
    }

    private String routingType;

    private List<String> roadAvoidanceFeatures;

    private String optimizationMode;

    private double maxSpeedInKmPerHour;

    private String orgId;

    private TruckConstraints truckConstraints;


    public String getRoutingType() {
        return routingType;
    }

    public void setRoutingType(String routingType) {
        this.routingType = routingType;
    }

    public List<String> getRoadAvoidanceFeatures() {
        return roadAvoidanceFeatures;
    }

    public void setRoadAvoidanceFeatures(List<String> roadAvoidanceFeatures) {
        this.roadAvoidanceFeatures = roadAvoidanceFeatures;
    }

    public String getOptimizationMode() {
        return optimizationMode;
    }

    public void setOptimizationMode(String optimizationMode) {
        this.optimizationMode = optimizationMode;
    }

    public double getMaxSpeedInKmPerHour() {
        return maxSpeedInKmPerHour;
    }

    public void setMaxSpeedInKmPerHour(double maxSpeedInKmPerHour) {
        this.maxSpeedInKmPerHour = maxSpeedInKmPerHour;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public TruckConstraints getTruckConstraints() {
        return truckConstraints;
    }

    public void setTruckConstraints(TruckConstraints truckConstraints) {
        this.truckConstraints = truckConstraints;
    }
}
