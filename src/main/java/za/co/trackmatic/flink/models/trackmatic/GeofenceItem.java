package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a geofence item, which includes details about a specific geofence
 * such as its identifier, zone, organization, name, entrance location, and shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeofenceItem implements Serializable  {

    /** Unique identifier for the geofence item */
    private String id;

    /** Identifier of the zone to which this geofence belongs */
    private String zoneId;

    /** Identifier of the organization that owns this geofence */
    private String orgId;

    /** Indicates if this geofence is adhoc (temporary or user-defined) */
    private boolean adhoc;

    /** Name of the geofence */
    private String name;

    /** The entrance location of the geofence, typically a LatLong coordinate */
    private LatLong entrance;

    /** The shape details of the geofence */
    private Geofences.Shape shape;

    public GeofenceItem() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LatLong getEntrance() {
        return entrance;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public boolean isAdhoc() {
        return adhoc;
    }

    public void setAdhoc(boolean adhoc) {
        this.adhoc = adhoc;
    }

    public void setEntrance(LatLong entrance) {
        this.entrance = entrance;
    }

    public Geofences.Shape getShape() {
        return shape;
    }

    public void setShape(Geofences.Shape shape) {
        this.shape = shape;
    }
}
