package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents geofence data including shape, location, and organizational grouping.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Geofences implements Serializable {

    /**
     * Represents the shape data of a geofence such as center point, markers (points), and radius.
     * Includes internal bounding box tracking for polygons.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShapeData implements Serializable {

        /** Center coordinate of the geofence shape */
        private LatLong center;

        /** List of marker coordinates that define the geofence shape (e.g., polygon vertices) */
        private List<LatLong> markers;

        /** Radius if the shape is circular */
        private double radius;

        /**
         * This is not a graphql field. It's for internal processing only.
         * Currently, it's used to track bounding box coords for polygons
         */
        private MinMaxLatLong minMax;

        public MinMaxLatLong getMinMax() {
            return minMax;
        }

        public void setMinMax(MinMaxLatLong minMax) {
            this.minMax = minMax;
        }

        public ShapeData() {

        }

        public LatLong getCenter() {
            return center;
        }

        public void setCenter(LatLong center) {
            this.center = center;
        }

        public List<LatLong> getMarkers() {
            return markers;
        }

        public void setMarkers(List<LatLong> markers) {
            this.markers = markers;
        }

        public double getRadius() {
            return radius;
        }

        public void setRadius(double radius) {
            this.radius = radius;
        }
    }

    /**
     * Represents minimum and maximum LatLong coordinates forming a bounding box.
     * Used internally to optimize polygon bounding calculations.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinMaxLatLong implements Serializable {

        /** Minimum coordinate (southwest corner) */
        private LatLong min = new LatLong();

        /** Maximum coordinate (northeast corner) */
        private LatLong max = new LatLong();

        public MinMaxLatLong() {

        }

        public MinMaxLatLong(LatLong min, LatLong max) {
            this.min = min;
            this.max = max;
        }

        public LatLong getMin() {
            return min;
        }

        public void setMin(LatLong min) {
            this.min = min;
        }

        public LatLong getMax() {
            return max;
        }

        public void setMax(LatLong max) {
            this.max = max;
        }
    }

    /**
     * Represents the shape of a geofence, including type (radius or polygon) and shape data.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Shape implements Serializable{

        /** Constant indicating the shape is a circle with a radius */
        public static final String SHAPE_TYPE_RADIUS = "RADIUS";

        /** Constant indicating the shape is a polygon */
        public static final String SHAPE_TYPE_POLYGON = "POLYGON";

        /** The type of the shape, e.g., RADIUS or POLYGON */
        private String type;

        /** Data specific to the shape type (center, markers, radius) */
        private ShapeData data;

        public Shape() {

        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public ShapeData getData() {
            return data;
        }

        public void setData(ShapeData data) {
            this.data = data;
        }
    }

    /**
     * Wrapper for geofence location data containing a list of GeofenceItem objects.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Locations implements Serializable {

        /** List of geofence items */
        private List<GeofenceItem> data;

        public Locations() {}

        public List<GeofenceItem> getData() {
            return data;
        }

        public void setData(List<GeofenceItem> data) {
            this.data = data;
        }
    }

    /**
     * Represents an organization’s geofence data grouping, which contains locations.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GetOrg implements Serializable {

        /** Locations associated with this organization */
        private Locations locations;

        public GetOrg() {

        }

        public Locations getLocations() {
            return locations;
        }

        public void setLocations(Locations locations) {
            this.locations = locations;
        }
    }

    /**
     * Wrapper class for the GetOrg data, to support nested GraphQL-like response structure.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GetOrgWrapper implements Serializable {

        /** Organization geofence data */
        private GetOrg getOrg;

        public GetOrgWrapper() {

        }

        public GetOrg getGetOrg() {
            return getOrg;
        }

        public void setGetOrg(GetOrg getOrg) {
            this.getOrg = getOrg;
        }
    }

    /** The top-level data wrapper containing organization geofence data */
    private GetOrgWrapper data;

    public Geofences() {}

    public GetOrgWrapper getData() {
        return data;
    }

    public void setData(GetOrgWrapper data) {
        this.data = data;
    }
}
