package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;

/**
 * Metadata used to enrich raw data.
 * <p>
 * Contains information about organization, asset, site IDs, control rooms,
 * stop IDs, routing violations, and active load related to assets.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TmMetadata implements Serializable  {

    /**
     * The organization identifier related to the Thermo King data.
     */
    private String orgId;

    /**
     * The asset identifier associated with the data.
     */
    private String assetId;

    /**
     * List of site identifiers relevant to this metadata.
     */
    private List<String> siteIds;

    /**
     * List of control rooms connected to this metadata.
     */
    private List<ControlRoom> controlRooms;

    /**
     * List of stop identifiers that is part of the load.
     */
    private List<String> stopIds;

    /**
     * Routing violations associated with this metadata.
     */
    private RoutingViolations routingViolations;

    /**
     * The active load information linked to the data.
     */
    private ActiveLoad activeLoad;


    public TmMetadata() {

    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public List<String> getSiteIds() {
        return siteIds;
    }

    public void setSiteIds(List<String> siteIds) {
        this.siteIds = siteIds;
    }

    public List<ControlRoom> getControlRooms() {
        return controlRooms;
    }

    public void setControlRooms(List<ControlRoom> controlRooms) {
        this.controlRooms = controlRooms;
    }

    public List<String> getStopIds() {
        return stopIds;
    }

    public void setStopIds(List<String> stopIds) {
        this.stopIds = stopIds;
    }

    public RoutingViolations getRoutingViolations() {
        return routingViolations;
    }

    public void setRoutingViolations(RoutingViolations routingViolations) {
        this.routingViolations = routingViolations;
    }

    public ActiveLoad getActiveLoad() {
        return activeLoad;
    }

    public void setActiveLoad(ActiveLoad activeLoad) {
        this.activeLoad = activeLoad;
    }
}
